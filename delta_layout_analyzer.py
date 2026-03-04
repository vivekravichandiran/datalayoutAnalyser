# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table Layout Analyzer
# MAGIC 
# MAGIC ## Overview
# MAGIC This production-grade notebook analyzes Delta table storage layout and history to generate optimization recommendations.
# MAGIC 
# MAGIC **Features:**
# MAGIC - Analyzes file layout and detects small files with percentage breakdown
# MAGIC - Detects partition skew and file size skew with severity levels
# MAGIC - Analyzes Delta history for OPTIMIZE, VACUUM, and MERGE operations
# MAGIC - Checks for Liquid Clustering on smaller tables
# MAGIC - Validates partitioning strategy for large tables
# MAGIC - Generates actionable optimization recommendations
# MAGIC - Supports Unity Catalog tables (managed and external)
# MAGIC - Scales to thousands of tables
# MAGIC 
# MAGIC **Author:** Databricks Platform Engineering  
# MAGIC **Version:** 2.0.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC Configure the analysis parameters below.

# COMMAND ----------

# DBTITLE 1,Configuration Parameters
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime, timedelta

@dataclass
class AnalyzerConfig:
    """Configuration for Delta Layout Analyzer."""
    
    # Target table configuration
    catalog: str = "myn_monitor_demo"
    schema: str = "observability"
    table: str = "table_name"
    
    # Scan mode: "single_table", "schema", or "catalog"
    scan_mode: str = "single_table"
    
    # File size thresholds (in MB)
    small_file_severe_mb: int = 32
    small_file_acceptable_mb: int = 128
    optimal_file_size_min_mb: int = 512
    optimal_file_size_max_mb: int = 1024
    
    # Skew thresholds
    file_skew_threshold: float = 10.0
    partition_skew_threshold: float = 10.0
    
    # Skew severity thresholds
    skew_low_threshold: float = 3.0
    skew_medium_threshold: float = 5.0
    skew_high_threshold: float = 10.0
    
    # Small files percentage thresholds
    small_files_pct_warning: float = 20.0  # Warning if >20% small files
    small_files_pct_critical: float = 50.0  # Critical if >50% small files
    
    # Health check thresholds
    max_partition_count: int = 5000
    large_table_threshold_gb: int = 1000  # 1TB - tables above this need partitioning
    zorder_recommend_threshold_gb: int = 500
    optimize_stale_days: int = 30
    vacuum_stale_days: int = 30
    
    # Output configuration
    output_catalog: str = "myn_monitor_demo"
    output_schema: str = "observability"
    output_table: str = "delta_layout_analysis"
    persist_results: bool = True
    
    # Performance settings
    max_history_records: int = 1000
    parallel_table_analysis: bool = True
    
    def get_full_table_name(self) -> str:
        """Return fully qualified table name."""
        return f"{self.catalog}.{self.schema}.{self.table}"
    
    def get_output_table_name(self) -> str:
        """Return fully qualified output table name."""
        return f"{self.output_catalog}.{self.output_schema}.{self.output_table}"

# Initialize default configuration
config = AnalyzerConfig()

# Display configuration
print("=" * 60)
print("Delta Layout Analyzer Configuration")
print("=" * 60)
print(f"Catalog:     {config.catalog}")
print(f"Schema:      {config.schema}")
print(f"Table:       {config.table}")
print(f"Scan Mode:   {config.scan_mode}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Edit Configuration (Optional)
# MAGIC Uncomment and modify the following cell to customize your configuration.

# COMMAND ----------

# DBTITLE 1,Custom Configuration (Edit as needed)
# Uncomment and modify to customize configuration
# config = AnalyzerConfig(
#     catalog="your_catalog",
#     schema="your_schema",
#     table="your_table",
#     scan_mode="single_table",  # Options: "single_table", "schema", "catalog"
#     persist_results=True
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports and Utilities

# COMMAND ----------

# DBTITLE 1,Import Dependencies
import logging
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from functools import reduce
from enum import Enum
import json
import statistics

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, ArrayType, TimestampType, IntegerType, BooleanType
)
from delta.tables import DeltaTable

# Get Spark session
spark = SparkSession.builder.getOrCreate()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DeltaLayoutAnalyzer")

# COMMAND ----------

# DBTITLE 1,Enums and Data Classes for Issues
class IssueSeverity(Enum):
    """Severity levels for detected issues."""
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class IssueCategory(Enum):
    """Categories of issues detected."""
    SMALL_FILES = "small_files"
    FILE_SKEW = "file_skew"
    PARTITION_SKEW = "partition_skew"
    STALE_OPTIMIZE = "stale_optimize"
    STALE_VACUUM = "stale_vacuum"
    EXCESS_PARTITIONS = "excess_partitions"
    NO_PARTITIONING = "no_partitioning"
    NO_LIQUID_CLUSTERING = "no_liquid_clustering"
    NO_ZORDER = "no_zorder"


@dataclass
class TableIssue:
    """A detected issue with a Delta table."""
    issue_id: str
    category: str
    severity: str
    title: str
    details: str
    current_value: str
    threshold: str
    recommendation: str
    sql_command: Optional[str] = None


class HealthStatus(Enum):
    """Overall health status of a table."""
    HEALTHY = "healthy"
    NEEDS_ATTENTION = "needs_attention"  
    WARNING = "warning"
    CRITICAL = "critical"
    ERROR = "error"

# COMMAND ----------

# DBTITLE 1,Utility Functions
def safe_execute_sql(query: str, default=None):
    """
    Safely execute SQL query with error handling.
    
    Args:
        query: SQL query to execute
        default: Default value to return on error
        
    Returns:
        DataFrame or default value on error
    """
    try:
        return spark.sql(query)
    except Exception as e:
        logger.warning(f"SQL execution failed: {e}")
        return default


def bytes_to_mb(bytes_val: int) -> float:
    """Convert bytes to megabytes."""
    return round(bytes_val / (1024 * 1024), 2) if bytes_val else 0.0


def bytes_to_gb(bytes_val: int) -> float:
    """Convert bytes to gigabytes."""
    return round(bytes_val / (1024 * 1024 * 1024), 2) if bytes_val else 0.0


def format_timestamp(ts) -> Optional[str]:
    """Format timestamp to ISO string."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.isoformat()
    return str(ts)


def days_ago_str(days: Optional[int]) -> str:
    """Format days ago as human-readable string."""
    if days is None:
        return "Never"
    elif days == 0:
        return "Today"
    elif days == 1:
        return "Yesterday"
    else:
        return f"{days} days ago"


def calculate_percentile(values: List[float], percentile: float) -> float:
    """Calculate percentile of a list of values."""
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = int(len(sorted_values) * percentile / 100)
    return sorted_values[min(index, len(sorted_values) - 1)]


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers."""
    if denominator == 0:
        return default
    return numerator / denominator


def get_skew_severity(skew_ratio: float, config: AnalyzerConfig) -> str:
    """
    Determine skew severity based on ratio.
    
    Logic:
    - skew_ratio <= 3.0: NONE (no significant skew)
    - skew_ratio <= 5.0: LOW (minor skew, monitor)
    - skew_ratio <= 10.0: MEDIUM (moderate skew, consider action)
    - skew_ratio > 10.0: HIGH (significant skew, action required)
    """
    if skew_ratio <= config.skew_low_threshold:
        return IssueSeverity.NONE.value
    elif skew_ratio <= config.skew_medium_threshold:
        return IssueSeverity.LOW.value
    elif skew_ratio <= config.skew_high_threshold:
        return IssueSeverity.MEDIUM.value
    else:
        return IssueSeverity.HIGH.value

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Table Discovery

# COMMAND ----------

# DBTITLE 1,Table Discovery Functions
def discover_tables(config: AnalyzerConfig) -> List[Dict[str, str]]:
    """
    Discover tables based on scan mode configuration.
    
    Args:
        config: Analyzer configuration
        
    Returns:
        List of table dictionaries with catalog, schema, table keys
    """
    tables = []
    
    if config.scan_mode == "single_table":
        tables.append({
            "catalog": config.catalog,
            "schema": config.schema,
            "table": config.table
        })
        logger.info(f"Single table mode: {config.get_full_table_name()}")
        
    elif config.scan_mode == "schema":
        tables = list_schema_tables(config.catalog, config.schema)
        logger.info(f"Schema mode: Found {len(tables)} tables in {config.catalog}.{config.schema}")
        
    elif config.scan_mode == "catalog":
        tables = list_catalog_tables(config.catalog)
        logger.info(f"Catalog mode: Found {len(tables)} tables in {config.catalog}")
        
    else:
        raise ValueError(f"Invalid scan_mode: {config.scan_mode}")
    
    return tables


def list_schema_tables(catalog: str, schema: str) -> List[Dict[str, str]]:
    """
    List all tables in a schema.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        
    Returns:
        List of table dictionaries
    """
    tables = []
    try:
        df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
        for row in df.collect():
            table_name = row.tableName if hasattr(row, 'tableName') else row[1]
            tables.append({
                "catalog": catalog,
                "schema": schema,
                "table": table_name
            })
    except Exception as e:
        logger.error(f"Error listing tables in {catalog}.{schema}: {e}")
    return tables


def list_catalog_tables(catalog: str) -> List[Dict[str, str]]:
    """
    List all tables in a catalog.
    
    Args:
        catalog: Catalog name
        
    Returns:
        List of table dictionaries
    """
    tables = []
    try:
        schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog}")
        for schema_row in schemas_df.collect():
            schema_name = schema_row.databaseName if hasattr(schema_row, 'databaseName') else schema_row[0]
            # Skip information_schema
            if schema_name.lower() == "information_schema":
                continue
            schema_tables = list_schema_tables(catalog, schema_name)
            tables.extend(schema_tables)
    except Exception as e:
        logger.error(f"Error listing tables in catalog {catalog}: {e}")
    return tables


def is_delta_table(catalog: str, schema: str, table: str) -> bool:
    """
    Check if a table is a Delta table.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        
    Returns:
        True if Delta table, False otherwise
    """
    try:
        full_name = f"{catalog}.{schema}.{table}"
        detail_df = spark.sql(f"DESCRIBE DETAIL {full_name}")
        format_val = detail_df.select("format").first()[0]
        return format_val.lower() == "delta"
    except Exception as e:
        logger.debug(f"Could not determine if {catalog}.{schema}.{table} is Delta: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Metadata Collection

# COMMAND ----------

# DBTITLE 1,Metadata Collection Function
def collect_table_metadata(catalog: str, schema: str, table: str) -> Dict[str, Any]:
    """
    Collect comprehensive metadata for a Delta table using DESCRIBE DETAIL.
    
    This function extracts table metadata without scanning table data,
    making it efficient for large tables.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        
    Returns:
        Dictionary containing table metadata
    """
    full_name = f"{catalog}.{schema}.{table}"
    metadata = {
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "full_name": full_name,
        "is_valid": False,
        "error": None
    }
    
    try:
        # Get table details using DESCRIBE DETAIL
        detail_df = spark.sql(f"DESCRIBE DETAIL {full_name}")
        detail_row = detail_df.first()
        
        if detail_row is None:
            metadata["error"] = "No detail information available"
            return metadata
        
        # Extract metadata fields
        metadata.update({
            "is_valid": True,
            "format": detail_row.format if hasattr(detail_row, 'format') else None,
            "location": detail_row.location if hasattr(detail_row, 'location') else None,
            "table_size_bytes": detail_row.sizeInBytes if hasattr(detail_row, 'sizeInBytes') else 0,
            "table_size_mb": bytes_to_mb(detail_row.sizeInBytes if hasattr(detail_row, 'sizeInBytes') else 0),
            "table_size_gb": bytes_to_gb(detail_row.sizeInBytes if hasattr(detail_row, 'sizeInBytes') else 0),
            "num_files": detail_row.numFiles if hasattr(detail_row, 'numFiles') else 0,
            "partition_columns": list(detail_row.partitionColumns) if hasattr(detail_row, 'partitionColumns') and detail_row.partitionColumns else [],
            "created_at": format_timestamp(detail_row.createdAt if hasattr(detail_row, 'createdAt') else None),
            "last_modified": format_timestamp(detail_row.lastModified if hasattr(detail_row, 'lastModified') else None),
            "table_properties": dict(detail_row.properties) if hasattr(detail_row, 'properties') and detail_row.properties else {},
            "min_reader_version": detail_row.minReaderVersion if hasattr(detail_row, 'minReaderVersion') else None,
            "min_writer_version": detail_row.minWriterVersion if hasattr(detail_row, 'minWriterVersion') else None,
        })
        
        # Check if it's a Delta table
        if metadata["format"] and metadata["format"].lower() != "delta":
            metadata["is_valid"] = False
            metadata["error"] = f"Not a Delta table (format: {metadata['format']})"
            
        logger.info(f"Collected metadata for {full_name}: {metadata['table_size_gb']} GB, {metadata['num_files']} files")
        
    except Exception as e:
        metadata["error"] = str(e)
        logger.error(f"Error collecting metadata for {full_name}: {e}")
    
    return metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Liquid Clustering Detection

# COMMAND ----------

# DBTITLE 1,Liquid Clustering Detection
def detect_liquid_clustering(catalog: str, schema: str, table: str) -> Dict[str, Any]:
    """
    Detect if a table uses Liquid Clustering by checking SHOW CREATE TABLE output.
    
    Liquid Clustering is identified by the presence of CLUSTER BY clause in the DDL.
    
    Logic:
    - Execute SHOW CREATE TABLE
    - Parse the DDL for 'CLUSTER BY' clause
    - Extract clustering columns if present
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        
    Returns:
        Dictionary with liquid clustering information
    """
    full_name = f"{catalog}.{schema}.{table}"
    result = {
        "is_liquid_clustered": False,
        "clustering_columns": [],
        "ddl": None,
        "error": None
    }
    
    try:
        # Get the CREATE TABLE statement
        ddl_df = spark.sql(f"SHOW CREATE TABLE {full_name}")
        ddl_row = ddl_df.first()
        
        if ddl_row:
            ddl = ddl_row[0] if isinstance(ddl_row[0], str) else str(ddl_row[0])
            result["ddl"] = ddl
            
            # Check for CLUSTER BY clause (Liquid Clustering)
            # Pattern matches: CLUSTER BY (col1, col2) or CLUSTER BY col1
            cluster_pattern = r'CLUSTER\s+BY\s*\(([^)]+)\)|CLUSTER\s+BY\s+(\w+)'
            match = re.search(cluster_pattern, ddl, re.IGNORECASE)
            
            if match:
                result["is_liquid_clustered"] = True
                # Extract column names
                cols_str = match.group(1) or match.group(2)
                if cols_str:
                    # Parse column names, handling quotes and spaces
                    cols = [col.strip().strip('`"\'') for col in cols_str.split(',')]
                    result["clustering_columns"] = [c for c in cols if c]
                    
        logger.info(f"Liquid clustering check for {full_name}: {result['is_liquid_clustered']}")
        
    except Exception as e:
        result["error"] = str(e)
        logger.debug(f"Could not check liquid clustering for {full_name}: {e}")
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. File Layout Analysis

# COMMAND ----------

# DBTITLE 1,File Layout Analysis Functions
def get_file_sizes_from_dbutils(table_path: str, sample_size: int = 50000) -> Tuple[List[int], List[Dict[str, Any]]]:
    """
    Get file sizes using dbutils.fs.ls for accurate file size information.
    
    This is the most reliable way to get actual file sizes from the Delta table location.
    dbutils.fs.ls returns FileInfo objects with path, name, size, and modificationTime.
    
    Example FileInfo:
        FileInfo(path='s3://bucket/table/part-00000.parquet', 
                 name='part-00000.parquet', 
                 size=46804904, 
                 modificationTime=1772614999000)
    
    Args:
        table_path: Path to Delta table location
        sample_size: Maximum number of files to process (for very large tables)
        
    Returns:
        Tuple of (list of file sizes in bytes, list of file info dicts)
    """
    file_sizes = []
    file_infos = []
    
    try:
        # Use dbutils.fs.ls to list all files in the table location
        all_files = dbutils.fs.ls(table_path)
        
        # Process files recursively (for partitioned tables)
        files_to_process = list(all_files)
        parquet_files = []
        
        # Iterate through directories to find all parquet files
        while files_to_process:
            current = files_to_process.pop(0)
            
            # Skip _delta_log directory and other metadata
            if current.name.startswith('_'):
                continue
            
            # If it's a directory (partitioned tables), list its contents
            if current.name.endswith('/'):
                try:
                    sub_files = dbutils.fs.ls(current.path)
                    files_to_process.extend(sub_files)
                except Exception:
                    pass
            # If it's a parquet file, add to our list
            elif current.name.endswith('.parquet'):
                parquet_files.append(current)
                
                # Stop if we've collected enough files for sampling
                if len(parquet_files) >= sample_size:
                    break
        
        # Extract sizes and info from parquet files
        for file_info in parquet_files:
            file_sizes.append(file_info.size)
            file_infos.append({
                "path": file_info.path,
                "name": file_info.name,
                "size": file_info.size,
                "size_mb": bytes_to_mb(file_info.size),
                "modification_time": file_info.modificationTime
            })
        
        logger.info(f"Retrieved {len(file_sizes)} file sizes from {table_path} using dbutils.fs.ls")
        
    except NameError:
        # dbutils not available (running outside Databricks)
        logger.warning("dbutils not available - falling back to Delta log parsing")
        return [], []
    except Exception as e:
        logger.warning(f"Error using dbutils.fs.ls for {table_path}: {e}")
        return [], []
    
    return file_sizes, file_infos


def analyze_file_layout(table_path: str, config: AnalyzerConfig) -> Dict[str, Any]:
    """
    Analyze file layout metrics for a Delta table.
    
    This function uses dbutils.fs.ls to get accurate individual file sizes,
    then calculates comprehensive statistics including:
    - Average, median, min, max file sizes
    - Standard deviation
    - Small files percentage
    - File size skew analysis
    
    Logic:
    1. Use dbutils.fs.ls to list all parquet files in table location
    2. Extract size from each FileInfo object
    3. Calculate statistics from actual file sizes
    4. Categorize files by size thresholds
    5. Calculate percentage of small files
    
    Args:
        table_path: Path to the Delta table (from DESCRIBE DETAIL location)
        config: Analyzer configuration
        
    Returns:
        Dictionary containing file layout statistics
    """
    result = {
        "total_files": 0,
        "total_size_bytes": 0,
        "avg_file_size_bytes": 0,
        "avg_file_size_mb": 0.0,
        "median_file_size_mb": 0.0,
        "min_file_size_mb": 0.0,
        "max_file_size_mb": 0.0,
        "std_dev_mb": 0.0,
        "small_files_count": 0,
        "small_files_pct": 0.0,
        "severe_small_files_count": 0,
        "severe_small_files_pct": 0.0,
        "optimal_files_count": 0,
        "optimal_files_pct": 0.0,
        "file_sizes": [],
        "file_details": [],
        "error": None
    }
    
    try:
        # Get basic info from DESCRIBE DETAIL first
        detail_df = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
        detail_row = detail_df.first()
        
        num_files_from_detail = detail_row.numFiles if detail_row else 0
        total_size_from_detail = detail_row.sizeInBytes if detail_row else 0
        
        if num_files_from_detail == 0:
            return result
        
        # PRIMARY METHOD: Use dbutils.fs.ls to get actual file sizes
        # This is the most reliable method and works with Unity Catalog tables
        file_sizes, file_details = get_file_sizes_from_dbutils(table_path)
        
        # NOTE: We don't fall back to Delta log parsing because:
        # 1. Unity Catalog tables may not grant direct file access to _delta_log
        # 2. Direct S3/ADLS access requires storage credentials that may not be available
        # If dbutils.fs.ls fails, we use DESCRIBE DETAIL averages (handled below)
        
        # Calculate statistics
        if file_sizes:
            total_size = sum(file_sizes)
            num_files = len(file_sizes)
            avg_size_bytes = total_size / num_files if num_files > 0 else 0
            
            file_sizes_mb = [bytes_to_mb(s) for s in file_sizes]
            
            result.update({
                "total_files": num_files,
                "total_size_bytes": total_size,
                "avg_file_size_bytes": avg_size_bytes,
                "avg_file_size_mb": round(statistics.mean(file_sizes_mb), 2) if file_sizes_mb else 0.0,
                "median_file_size_mb": round(statistics.median(file_sizes_mb), 2) if file_sizes_mb else 0.0,
                "min_file_size_mb": round(min(file_sizes_mb), 2) if file_sizes_mb else 0.0,
                "max_file_size_mb": round(max(file_sizes_mb), 2) if file_sizes_mb else 0.0,
                "std_dev_mb": round(statistics.stdev(file_sizes_mb), 2) if len(file_sizes_mb) > 1 else 0.0,
                "file_sizes": file_sizes,
                "file_details": file_details,
            })
            
            # Categorize files by size thresholds
            severe_threshold = config.small_file_severe_mb * 1024 * 1024  # bytes
            acceptable_threshold = config.small_file_acceptable_mb * 1024 * 1024
            optimal_min = config.optimal_file_size_min_mb * 1024 * 1024
            optimal_max = config.optimal_file_size_max_mb * 1024 * 1024
            
            severe_count = sum(1 for s in file_sizes if s < severe_threshold)
            small_count = sum(1 for s in file_sizes if s < acceptable_threshold)
            optimal_count = sum(1 for s in file_sizes if optimal_min <= s <= optimal_max)
            
            result.update({
                "severe_small_files_count": severe_count,
                "severe_small_files_pct": round(100 * severe_count / num_files, 2),
                "small_files_count": small_count,
                "small_files_pct": round(100 * small_count / num_files, 2),
                "optimal_files_count": optimal_count,
                "optimal_files_pct": round(100 * optimal_count / num_files, 2),
            })
            
            logger.info(f"Analyzed file layout: {num_files} files, avg size: {result['avg_file_size_mb']} MB, "
                       f"median: {result['median_file_size_mb']} MB, "
                       f"small files: {result['small_files_pct']}%")
        else:
            # If we couldn't get individual file sizes, use DESCRIBE DETAIL averages
            # This happens when:
            # 1. dbutils.fs.ls is not available (outside Databricks)
            # 2. Storage access is restricted (Unity Catalog without external location access)
            avg_size_bytes = total_size_from_detail / num_files_from_detail if num_files_from_detail > 0 else 0
            avg_size_mb = bytes_to_mb(avg_size_bytes)
            
            result.update({
                "total_files": num_files_from_detail,
                "total_size_bytes": total_size_from_detail,
                "avg_file_size_bytes": avg_size_bytes,
                "avg_file_size_mb": avg_size_mb,
                # Use average as estimate for median (not accurate but best we have)
                "median_file_size_mb": avg_size_mb,
                # We can still flag small files based on average
                # If average is small, most files are likely small
                "small_files_pct": 100.0 if avg_size_mb < config.small_file_acceptable_mb else 0.0,
                "severe_small_files_pct": 100.0 if avg_size_mb < config.small_file_severe_mb else 0.0,
            })
            logger.info(f"Using DESCRIBE DETAIL averages (individual file listing not available): "
                       f"{num_files_from_detail} files, avg size: {avg_size_mb} MB")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error analyzing file layout for {table_path}: {e}")
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Partition Analysis

# COMMAND ----------

# DBTITLE 1,Partition Analysis Functions
def analyze_partitions(
    catalog: str, 
    schema: str, 
    table: str, 
    partition_columns: List[str],
    config: AnalyzerConfig
) -> Dict[str, Any]:
    """
    Analyze partition distribution and detect partition skew.
    
    Uses metadata queries to analyze partitions without full table scans.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        partition_columns: List of partition column names
        config: Analyzer configuration
        
    Returns:
        Dictionary containing partition analysis results
    """
    result = {
        "is_partitioned": bool(partition_columns),
        "partition_count": 0,
        "partition_columns": partition_columns,
        "largest_partition_size_mb": 0.0,
        "smallest_partition_size_mb": 0.0,
        "median_partition_size_mb": 0.0,
        "avg_partition_size_mb": 0.0,
        "partition_skew_ratio": 0.0,
        "partition_skew_severity": IssueSeverity.NONE.value,
        "has_partition_skew": False,
        "partition_distribution": [],
        "error": None
    }
    
    if not partition_columns:
        result["error"] = "Table is not partitioned"
        return result
    
    full_name = f"{catalog}.{schema}.{table}"
    
    try:
        # Get partition information using SHOW PARTITIONS
        try:
            partitions_df = spark.sql(f"SHOW PARTITIONS {full_name}")
            result["partition_count"] = partitions_df.count()
        except Exception:
            # SHOW PARTITIONS might not work for all tables
            # Fall back to counting distinct partition values
            partition_cols_str = ", ".join(partition_columns)
            count_df = spark.sql(f"""
                SELECT COUNT(DISTINCT concat_ws('/', {partition_cols_str})) as partition_count
                FROM {full_name}
            """)
            result["partition_count"] = count_df.first()[0] or 0
        
        # For partition size distribution, we need to aggregate by partition
        # This is done efficiently using Delta metadata when possible
        if result["partition_count"] > 0 and result["partition_count"] <= 10000:
            partition_sizes = get_partition_sizes(catalog, schema, table, partition_columns)
            
            if partition_sizes:
                sizes_mb = [bytes_to_mb(s) for s in partition_sizes]
                
                result.update({
                    "largest_partition_size_mb": round(max(sizes_mb), 2),
                    "smallest_partition_size_mb": round(min(sizes_mb), 2),
                    "median_partition_size_mb": round(statistics.median(sizes_mb), 2),
                    "avg_partition_size_mb": round(statistics.mean(sizes_mb), 2),
                })
                
                # Calculate skew ratio
                median = result["median_partition_size_mb"]
                if median > 0:
                    skew_ratio = result["largest_partition_size_mb"] / median
                    result["partition_skew_ratio"] = round(skew_ratio, 2)
                    result["has_partition_skew"] = skew_ratio > config.partition_skew_threshold
                    result["partition_skew_severity"] = get_skew_severity(skew_ratio, config)
        
        logger.info(f"Analyzed partitions for {full_name}: {result['partition_count']} partitions")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error analyzing partitions for {full_name}: {e}")
    
    return result


def get_partition_sizes(
    catalog: str, 
    schema: str, 
    table: str, 
    partition_columns: List[str],
    sample_limit: int = 1000
) -> List[int]:
    """
    Get approximate partition sizes using dbutils.fs.ls or DESCRIBE DETAIL estimates.
    
    This function avoids direct Delta log access to work with Unity Catalog tables
    that may have restricted storage access.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        partition_columns: Partition column names
        sample_limit: Maximum number of partitions to analyze
        
    Returns:
        List of partition sizes in bytes
    """
    full_name = f"{catalog}.{schema}.{table}"
    
    try:
        # Get table details
        detail_df = spark.sql(f"DESCRIBE DETAIL {full_name}")
        detail_row = detail_df.first()
        location = detail_row.location if detail_row else None
        total_size = detail_row.sizeInBytes if detail_row else 0
        
        if not location:
            return []
        
        # METHOD 1: Try to use dbutils.fs.ls to get partition directory sizes
        try:
            partition_sizes = {}
            all_items = dbutils.fs.ls(location)
            
            for item in all_items:
                # Skip _delta_log and other metadata
                if item.name.startswith('_'):
                    continue
                
                # For partitioned tables, directories are partition folders
                if item.name.endswith('/'):
                    # List files in this partition to sum sizes
                    try:
                        partition_files = dbutils.fs.ls(item.path)
                        partition_size = sum(
                            f.size for f in partition_files 
                            if f.name.endswith('.parquet')
                        )
                        if partition_size > 0:
                            partition_sizes[item.name] = partition_size
                            
                        # Stop if we've collected enough partitions
                        if len(partition_sizes) >= sample_limit:
                            break
                    except Exception:
                        pass
            
            if partition_sizes:
                logger.info(f"Got {len(partition_sizes)} partition sizes using dbutils.fs.ls")
                return list(partition_sizes.values())
                
        except NameError:
            # dbutils not available
            pass
        except Exception as e:
            logger.debug(f"Could not get partition sizes using dbutils: {e}")
        
        # METHOD 2: Estimate based on total size and partition count
        try:
            partitions_df = spark.sql(f"SHOW PARTITIONS {full_name}")
            partition_count = partitions_df.count()
            if partition_count > 0:
                avg_size = total_size / partition_count
                # Return estimated sizes (all same since we can't get actual)
                logger.info(f"Estimating partition sizes from total size: {partition_count} partitions, avg {bytes_to_mb(avg_size)} MB")
                return [int(avg_size)] * min(partition_count, sample_limit)
        except Exception:
            pass
            
    except Exception as e:
        logger.debug(f"Error getting partition sizes for {full_name}: {e}")
    
    return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. File Size Skew Detection

# COMMAND ----------

# DBTITLE 1,File Size Skew Detection
def detect_file_skew(file_sizes: List[int], config: AnalyzerConfig) -> Dict[str, Any]:
    """
    Detect file size skew in a Delta table.
    
    File skew occurs when there's a large variance in file sizes,
    which can impact query performance.
    
    Logic:
    - Calculate skew_ratio = max_file_size / median_file_size
    - Determine severity based on thresholds:
      * ratio <= 3: NONE
      * ratio <= 5: LOW
      * ratio <= 10: MEDIUM  
      * ratio > 10: HIGH
    
    Args:
        file_sizes: List of file sizes in bytes
        config: Analyzer configuration
        
    Returns:
        Dictionary containing skew analysis results
    """
    result = {
        "file_skew_ratio": 0.0,
        "has_file_skew": False,
        "skew_severity": IssueSeverity.NONE.value,
        "coefficient_of_variation": 0.0,
        "skew_description": "No significant skew",
        "error": None
    }
    
    if not file_sizes or len(file_sizes) < 2:
        result["error"] = "Insufficient file data for skew analysis"
        return result
    
    try:
        sizes_mb = [bytes_to_mb(s) for s in file_sizes]
        
        median_size = statistics.median(sizes_mb)
        max_size = max(sizes_mb)
        min_size = min(sizes_mb)
        mean_size = statistics.mean(sizes_mb)
        std_dev = statistics.stdev(sizes_mb)
        
        # Calculate skew ratio
        skew_ratio = max_size / median_size if median_size > 0 else 0
        result["file_skew_ratio"] = round(skew_ratio, 2)
        result["has_file_skew"] = skew_ratio > config.file_skew_threshold
        
        # Calculate coefficient of variation
        cv = (std_dev / mean_size * 100) if mean_size > 0 else 0
        result["coefficient_of_variation"] = round(cv, 2)
        
        # Determine severity and description
        severity = get_skew_severity(skew_ratio, config)
        result["skew_severity"] = severity
        
        # Generate description
        if severity == IssueSeverity.NONE.value:
            result["skew_description"] = "No significant skew - file sizes are well balanced"
        elif severity == IssueSeverity.LOW.value:
            result["skew_description"] = f"Minor skew detected (ratio: {skew_ratio:.1f}x) - monitor over time"
        elif severity == IssueSeverity.MEDIUM.value:
            result["skew_description"] = f"Moderate skew detected (ratio: {skew_ratio:.1f}x) - consider optimization"
        else:
            result["skew_description"] = f"Significant skew detected (ratio: {skew_ratio:.1f}x) - action required"
        
        logger.info(f"File skew analysis: ratio={skew_ratio:.2f}, severity={severity}")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error detecting file skew: {e}")
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Delta History Analysis

# COMMAND ----------

# DBTITLE 1,Delta History Analysis Functions
def analyze_delta_history(
    catalog: str, 
    schema: str, 
    table: str,
    config: AnalyzerConfig
) -> Dict[str, Any]:
    """
    Analyze Delta table history to understand maintenance patterns.
    
    Uses DESCRIBE HISTORY to extract operation history without scanning data.
    
    Logic:
    - Query DESCRIBE HISTORY for recent operations
    - Track timestamps for OPTIMIZE, VACUUM, MERGE, WRITE, DELETE, UPDATE
    - Calculate days since last maintenance operations
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        config: Analyzer configuration
        
    Returns:
        Dictionary containing history analysis results
    """
    full_name = f"{catalog}.{schema}.{table}"
    
    result = {
        "total_commits": 0,
        "last_write_timestamp": None,
        "last_optimize_timestamp": None,
        "last_vacuum_timestamp": None,
        "last_merge_timestamp": None,
        "optimize_count": 0,
        "vacuum_count": 0,
        "merge_count": 0,
        "write_count": 0,
        "delete_count": 0,
        "update_count": 0,
        "days_since_last_optimize": None,
        "days_since_last_vacuum": None,
        "days_since_last_write": None,
        "optimize_status": "Never run",
        "vacuum_status": "Never run",
        "operation_summary": {},
        "history_df": None,
        "error": None
    }
    
    try:
        # Get table history
        history_df = spark.sql(f"DESCRIBE HISTORY {full_name} LIMIT {config.max_history_records}")
        result["history_df"] = history_df
        result["total_commits"] = history_df.count()
        
        # Collect history for analysis
        history_rows = history_df.collect()
        
        # Initialize operation tracking
        operations = {}
        last_timestamps = {
            "WRITE": None,
            "OPTIMIZE": None,
            "VACUUM END": None,
            "VACUUM START": None,
            "MERGE": None,
            "DELETE": None,
            "UPDATE": None
        }
        
        # Process each history entry
        for row in history_rows:
            operation = row.operation.upper() if row.operation else "UNKNOWN"
            timestamp = row.timestamp
            
            # Count operations
            operations[operation] = operations.get(operation, 0) + 1
            
            # Track latest timestamp for each operation type
            for op_key in last_timestamps.keys():
                if op_key in operation:
                    if last_timestamps[op_key] is None or timestamp > last_timestamps[op_key]:
                        last_timestamps[op_key] = timestamp
        
        # Update result
        result["operation_summary"] = operations
        result["write_count"] = operations.get("WRITE", 0)
        result["optimize_count"] = operations.get("OPTIMIZE", 0)
        result["vacuum_count"] = operations.get("VACUUM END", 0) + operations.get("VACUUM START", 0)
        result["merge_count"] = operations.get("MERGE", 0)
        result["delete_count"] = operations.get("DELETE", 0)
        result["update_count"] = operations.get("UPDATE", 0)
        
        # Format timestamps
        result["last_write_timestamp"] = format_timestamp(last_timestamps.get("WRITE"))
        result["last_optimize_timestamp"] = format_timestamp(last_timestamps.get("OPTIMIZE"))
        result["last_vacuum_timestamp"] = format_timestamp(
            last_timestamps.get("VACUUM END") or last_timestamps.get("VACUUM START")
        )
        result["last_merge_timestamp"] = format_timestamp(last_timestamps.get("MERGE"))
        
        # Calculate days since last operations
        now = datetime.now()
        
        if last_timestamps.get("WRITE"):
            result["days_since_last_write"] = (now - last_timestamps["WRITE"]).days
            
        if last_timestamps.get("OPTIMIZE"):
            days = (now - last_timestamps["OPTIMIZE"]).days
            result["days_since_last_optimize"] = days
            result["optimize_status"] = days_ago_str(days)
        else:
            result["optimize_status"] = "Never run"
            
        if last_timestamps.get("VACUUM END") or last_timestamps.get("VACUUM START"):
            vacuum_ts = last_timestamps.get("VACUUM END") or last_timestamps.get("VACUUM START")
            days = (now - vacuum_ts).days
            result["days_since_last_vacuum"] = days
            result["vacuum_status"] = days_ago_str(days)
        else:
            result["vacuum_status"] = "Never run"
        
        logger.info(f"Analyzed history for {full_name}: {result['total_commits']} commits")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error analyzing history for {full_name}: {e}")
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. OPTIMIZE & ZORDER Detection

# COMMAND ----------

# DBTITLE 1,OPTIMIZE and ZORDER Detection
def detect_optimize_operations(history_df: DataFrame) -> Dict[str, Any]:
    """
    Detect OPTIMIZE operations and extract ZORDER columns from history.
    
    Parses operationParameters to identify ZORDER columns used in optimization.
    
    Args:
        history_df: DataFrame from DESCRIBE HISTORY
        
    Returns:
        Dictionary containing OPTIMIZE/ZORDER analysis
    """
    result = {
        "optimize_count": 0,
        "last_optimize_timestamp": None,
        "optimize_frequency_days": None,
        "zorder_columns": [],
        "zorder_usage_count": 0,
        "predicate_optimizations": 0,
        "auto_compact_enabled": False,
        "optimize_details": [],
        "error": None
    }
    
    if history_df is None:
        result["error"] = "No history data available"
        return result
    
    try:
        # Filter for OPTIMIZE operations
        optimize_df = history_df.filter(
            F.upper(F.col("operation")) == "OPTIMIZE"
        ).orderBy(F.col("timestamp").desc())
        
        optimize_count = optimize_df.count()
        result["optimize_count"] = optimize_count
        
        if optimize_count == 0:
            return result
        
        # Get OPTIMIZE details
        optimize_rows = optimize_df.collect()
        
        # Track ZORDER columns
        zorder_columns_set = set()
        optimize_timestamps = []
        
        for row in optimize_rows:
            timestamp = row.timestamp
            optimize_timestamps.append(timestamp)
            
            # Parse operationParameters for ZORDER
            if row.operationParameters:
                params = row.operationParameters
                
                # ZORDER columns are typically in 'zOrderBy' parameter
                if isinstance(params, dict):
                    zorder_str = params.get("zOrderBy", "")
                    predicate = params.get("predicate")
                    
                    if zorder_str:
                        # Parse ZORDER columns - format can vary
                        # e.g., "[\"col1\",\"col2\"]" or "col1, col2"
                        try:
                            if zorder_str.startswith("["):
                                cols = json.loads(zorder_str)
                            else:
                                cols = [c.strip().strip('"\'') for c in zorder_str.split(",")]
                            zorder_columns_set.update(cols)
                            result["zorder_usage_count"] += 1
                        except Exception:
                            # Handle other formats
                            cols = [c.strip() for c in zorder_str.replace("[", "").replace("]", "").replace('"', '').split(",")]
                            zorder_columns_set.update(c for c in cols if c)
                            result["zorder_usage_count"] += 1
                    
                    if predicate:
                        result["predicate_optimizations"] += 1
                
                # Store details for recent optimizations
                if len(result["optimize_details"]) < 5:
                    result["optimize_details"].append({
                        "timestamp": format_timestamp(timestamp),
                        "parameters": dict(params) if isinstance(params, dict) else str(params)
                    })
        
        # Set results
        result["zorder_columns"] = sorted(list(zorder_columns_set))
        result["last_optimize_timestamp"] = format_timestamp(optimize_timestamps[0]) if optimize_timestamps else None
        
        # Calculate optimize frequency
        if len(optimize_timestamps) >= 2:
            time_diffs = []
            for i in range(len(optimize_timestamps) - 1):
                diff = (optimize_timestamps[i] - optimize_timestamps[i + 1]).days
                time_diffs.append(diff)
            result["optimize_frequency_days"] = round(statistics.mean(time_diffs), 1)
        
        logger.info(f"OPTIMIZE detection: {optimize_count} optimizations, ZORDER columns: {result['zorder_columns']}")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error detecting OPTIMIZE operations: {e}")
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Table Growth Analysis

# COMMAND ----------

# DBTITLE 1,Table Growth Analysis
def analyze_table_growth(history_df: DataFrame, table_size_bytes: int) -> Dict[str, Any]:
    """
    Analyze table growth patterns using Delta history.
    
    Computes daily write volumes and growth rates from commit history.
    
    Args:
        history_df: DataFrame from DESCRIBE HISTORY
        table_size_bytes: Current table size in bytes
        
    Returns:
        Dictionary containing growth analysis
    """
    result = {
        "daily_write_count": 0.0,
        "avg_write_size_mb": 0.0,
        "estimated_daily_growth_mb": 0.0,
        "growth_rate_pct_daily": 0.0,
        "write_patterns": {},
        "busiest_day": None,
        "error": None
    }
    
    if history_df is None:
        result["error"] = "No history data available"
        return result
    
    try:
        # Filter WRITE operations
        write_df = history_df.filter(
            F.upper(F.col("operation")).isin(["WRITE", "MERGE", "UPDATE", "DELETE"])
        )
        
        write_count = write_df.count()
        
        if write_count == 0:
            return result
        
        # Get write timestamps
        write_rows = write_df.select(
            F.col("timestamp"),
            F.col("operation"),
            F.col("operationMetrics")
        ).collect()
        
        # Analyze write patterns
        timestamps = [row.timestamp for row in write_rows]
        
        if len(timestamps) >= 2:
            # Calculate date range
            min_ts = min(timestamps)
            max_ts = max(timestamps)
            days_range = max((max_ts - min_ts).days, 1)
            
            # Daily write frequency
            result["daily_write_count"] = round(write_count / days_range, 2)
            
            # Analyze by day of week
            day_counts = {}
            for ts in timestamps:
                day_name = ts.strftime("%A")
                day_counts[day_name] = day_counts.get(day_name, 0) + 1
            
            result["write_patterns"] = day_counts
            result["busiest_day"] = max(day_counts, key=day_counts.get) if day_counts else None
        
        # Estimate write sizes from operation metrics
        total_bytes_written = 0
        writes_with_metrics = 0
        
        for row in write_rows:
            if row.operationMetrics:
                metrics = row.operationMetrics
                if isinstance(metrics, dict):
                    bytes_written = metrics.get("numOutputBytes") or metrics.get("numAddedBytes") or 0
                    if isinstance(bytes_written, str):
                        bytes_written = int(bytes_written) if bytes_written.isdigit() else 0
                    total_bytes_written += bytes_written
                    writes_with_metrics += 1
        
        if writes_with_metrics > 0:
            result["avg_write_size_mb"] = round(bytes_to_mb(total_bytes_written / writes_with_metrics), 2)
            
            if len(timestamps) >= 2:
                days_range = max((max(timestamps) - min(timestamps)).days, 1)
                result["estimated_daily_growth_mb"] = round(bytes_to_mb(total_bytes_written / days_range), 2)
                
                if table_size_bytes > 0:
                    daily_bytes = total_bytes_written / days_range
                    result["growth_rate_pct_daily"] = round(100 * daily_bytes / table_size_bytes, 4)
        
        logger.info(f"Growth analysis: {result['daily_write_count']} writes/day, {result['estimated_daily_growth_mb']} MB/day")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error analyzing table growth: {e}")
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Issue Detection Engine

# COMMAND ----------

# DBTITLE 1,Issue Detection Functions
def detect_all_issues(
    full_table_name: str,
    metadata: Dict[str, Any],
    file_layout: Dict[str, Any],
    file_skew: Dict[str, Any],
    partition_analysis: Dict[str, Any],
    history_analysis: Dict[str, Any],
    optimize_info: Dict[str, Any],
    liquid_clustering: Dict[str, Any],
    config: AnalyzerConfig
) -> List[TableIssue]:
    """
    Detect all issues with a Delta table and create structured issue list.
    
    This function evaluates all metrics and produces a clear list of issues
    with severity, details, and recommendations.
    
    Args:
        full_table_name: Fully qualified table name
        metadata: Table metadata
        file_layout: File layout analysis
        file_skew: File skew analysis
        partition_analysis: Partition analysis
        history_analysis: History analysis
        optimize_info: OPTIMIZE detection results
        liquid_clustering: Liquid clustering detection results
        config: Analyzer configuration
        
    Returns:
        List of TableIssue objects
    """
    issues = []
    
    table_size_gb = metadata.get("table_size_gb", 0)
    avg_file_size_mb = file_layout.get("avg_file_size_mb", 0)
    small_files_pct = file_layout.get("small_files_pct", 0)
    severe_small_files_pct = file_layout.get("severe_small_files_pct", 0)
    partition_columns = metadata.get("partition_columns", [])
    is_partitioned = bool(partition_columns)
    zorder_columns = optimize_info.get("zorder_columns", [])
    is_liquid_clustered = liquid_clustering.get("is_liquid_clustered", False)
    clustering_columns = liquid_clustering.get("clustering_columns", [])
    
    # =========================================================================
    # ISSUE 1: Small Files Detection
    # =========================================================================
    # Logic: 
    # - Critical if avg file size < 32MB OR >50% of files are small
    # - Warning if avg file size < 128MB OR >20% of files are small
    # =========================================================================
    if metadata.get("num_files", 0) > 0 and avg_file_size_mb > 0:
        if avg_file_size_mb < config.small_file_severe_mb or severe_small_files_pct > config.small_files_pct_critical:
            severity = IssueSeverity.CRITICAL.value
            issues.append(TableIssue(
                issue_id="SMALL_FILES_CRITICAL",
                category=IssueCategory.SMALL_FILES.value,
                severity=severity,
                title="Critical Small Files Issue",
                details=f"Average file size: {avg_file_size_mb} MB (threshold: {config.small_file_severe_mb} MB). "
                       f"Small files (<{config.small_file_severe_mb}MB): {severe_small_files_pct}% of total files. "
                       f"Small files (<{config.small_file_acceptable_mb}MB): {small_files_pct}% of total files. "
                       f"Total files: {metadata.get('num_files', 0):,}",
                current_value=f"{avg_file_size_mb} MB avg, {severe_small_files_pct}% severe small files",
                threshold=f">{config.small_file_severe_mb} MB avg, <{config.small_files_pct_critical}% small files",
                recommendation="Run OPTIMIZE immediately to compact small files. Consider adjusting write batch sizes.",
                sql_command=f"OPTIMIZE {full_table_name};"
            ))
        elif avg_file_size_mb < config.small_file_acceptable_mb or small_files_pct > config.small_files_pct_warning:
            severity = IssueSeverity.MEDIUM.value
            issues.append(TableIssue(
                issue_id="SMALL_FILES_WARNING",
                category=IssueCategory.SMALL_FILES.value,
                severity=severity,
                title="Small Files Issue",
                details=f"Average file size: {avg_file_size_mb} MB (threshold: {config.small_file_acceptable_mb} MB). "
                       f"Small files (<{config.small_file_acceptable_mb}MB): {small_files_pct}% of total files. "
                       f"Total files: {metadata.get('num_files', 0):,}",
                current_value=f"{avg_file_size_mb} MB avg, {small_files_pct}% small files",
                threshold=f">{config.small_file_acceptable_mb} MB avg, <{config.small_files_pct_warning}% small files",
                recommendation="Run OPTIMIZE to compact files. Schedule regular optimization jobs.",
                sql_command=f"OPTIMIZE {full_table_name};"
            ))
    
    # =========================================================================
    # ISSUE 2: File Skew Detection
    # =========================================================================
    # Logic:
    # - skew_ratio = max_file_size / median_file_size
    # - High: ratio > 10, Medium: ratio > 5, Low: ratio > 3
    # =========================================================================
    file_skew_ratio = file_skew.get("file_skew_ratio", 0)
    skew_severity = file_skew.get("skew_severity", IssueSeverity.NONE.value)
    
    if skew_severity != IssueSeverity.NONE.value:
        issues.append(TableIssue(
            issue_id=f"FILE_SKEW_{skew_severity.upper()}",
            category=IssueCategory.FILE_SKEW.value,
            severity=skew_severity,
            title=f"File Size Skew ({skew_severity.capitalize()})",
            details=f"File skew ratio: {file_skew_ratio}x (max/median). "
                   f"Largest file: {file_layout.get('max_file_size_mb', 0)} MB, "
                   f"Median file: {file_layout.get('median_file_size_mb', 0)} MB, "
                   f"Smallest file: {file_layout.get('min_file_size_mb', 0)} MB. "
                   f"Coefficient of variation: {file_skew.get('coefficient_of_variation', 0)}%",
            current_value=f"{file_skew_ratio}x skew ratio",
            threshold=f"<{config.skew_low_threshold}x (none), <{config.skew_high_threshold}x (acceptable)",
            recommendation="Investigate data ingestion patterns. Repartition data before writing. Consider using coalesce() or repartition().",
            sql_command=None
        ))
    
    # =========================================================================
    # ISSUE 3: Stale OPTIMIZE
    # =========================================================================
    # Logic:
    # - Critical if OPTIMIZE never run
    # - Warning if last OPTIMIZE > 30 days ago
    # =========================================================================
    days_since_optimize = history_analysis.get("days_since_last_optimize")
    if days_since_optimize is None:
        issues.append(TableIssue(
            issue_id="OPTIMIZE_NEVER_RUN",
            category=IssueCategory.STALE_OPTIMIZE.value,
            severity=IssueSeverity.HIGH.value,
            title="OPTIMIZE Never Run",
            details="The table has never been optimized. OPTIMIZE compacts small files and improves query performance.",
            current_value="Never",
            threshold=f"Within last {config.optimize_stale_days} days",
            recommendation="Run OPTIMIZE to compact files. Set up a scheduled job for regular optimization.",
            sql_command=f"OPTIMIZE {full_table_name};" + (f" ZORDER BY ({', '.join(zorder_columns)});" if zorder_columns else "")
        ))
    elif days_since_optimize > config.optimize_stale_days:
        issues.append(TableIssue(
            issue_id="OPTIMIZE_STALE",
            category=IssueCategory.STALE_OPTIMIZE.value,
            severity=IssueSeverity.MEDIUM.value,
            title="Stale OPTIMIZE",
            details=f"Last OPTIMIZE was {days_since_optimize} days ago ({history_analysis.get('last_optimize_timestamp', 'Unknown')}). "
                   f"Threshold: {config.optimize_stale_days} days.",
            current_value=f"{days_since_optimize} days ago",
            threshold=f"Within last {config.optimize_stale_days} days",
            recommendation="Run OPTIMIZE to maintain query performance. Consider scheduling regular optimization.",
            sql_command=f"OPTIMIZE {full_table_name};" + (f" ZORDER BY ({', '.join(zorder_columns)});" if zorder_columns else "")
        ))
    
    # =========================================================================
    # ISSUE 4: Stale VACUUM
    # =========================================================================
    # Logic:
    # - Warning if VACUUM never run
    # - Warning if last VACUUM > 30 days ago
    # =========================================================================
    days_since_vacuum = history_analysis.get("days_since_last_vacuum")
    if days_since_vacuum is None:
        issues.append(TableIssue(
            issue_id="VACUUM_NEVER_RUN",
            category=IssueCategory.STALE_VACUUM.value,
            severity=IssueSeverity.MEDIUM.value,
            title="VACUUM Never Run",
            details="The table has never been vacuumed. VACUUM removes old files and reclaims storage space.",
            current_value="Never",
            threshold=f"Within last {config.vacuum_stale_days} days",
            recommendation="Run VACUUM to remove old files and reclaim storage. Schedule regular vacuum jobs.",
            sql_command=f"VACUUM {full_table_name} RETAIN 168 HOURS;"
        ))
    elif days_since_vacuum > config.vacuum_stale_days:
        issues.append(TableIssue(
            issue_id="VACUUM_STALE",
            category=IssueCategory.STALE_VACUUM.value,
            severity=IssueSeverity.LOW.value,
            title="Stale VACUUM",
            details=f"Last VACUUM was {days_since_vacuum} days ago ({history_analysis.get('last_vacuum_timestamp', 'Unknown')}). "
                   f"Threshold: {config.vacuum_stale_days} days.",
            current_value=f"{days_since_vacuum} days ago",
            threshold=f"Within last {config.vacuum_stale_days} days",
            recommendation="Run VACUUM to reclaim storage space and remove old files.",
            sql_command=f"VACUUM {full_table_name} RETAIN 168 HOURS;"
        ))
    
    # =========================================================================
    # ISSUE 5: Large Table Without Partitioning (>1TB)
    # =========================================================================
    # Logic:
    # - If table size > 1TB and not partitioned, recommend partitioning
    # =========================================================================
    if table_size_gb >= config.large_table_threshold_gb and not is_partitioned:
        issues.append(TableIssue(
            issue_id="LARGE_TABLE_NO_PARTITION",
            category=IssueCategory.NO_PARTITIONING.value,
            severity=IssueSeverity.HIGH.value,
            title="Large Table Without Partitioning",
            details=f"Table size: {table_size_gb} GB exceeds {config.large_table_threshold_gb} GB threshold. "
                   f"Large tables should be partitioned for better query performance and maintenance.",
            current_value=f"{table_size_gb} GB, not partitioned",
            threshold=f"Tables >{config.large_table_threshold_gb} GB should be partitioned",
            recommendation="Consider partitioning by a date column (e.g., created_date) or another high-cardinality column. "
                          "Recreate the table with PARTITIONED BY clause.",
            sql_command=f"-- Recreate with partitioning:\n-- CREATE TABLE {full_table_name}_new PARTITIONED BY (partition_column) AS SELECT * FROM {full_table_name};"
        ))
    
    # =========================================================================
    # ISSUE 6: Smaller Table Without Liquid Clustering (<1TB)
    # =========================================================================
    # Logic:
    # - If table size < 1TB and not liquid clustered, recommend liquid clustering
    # - Check DDL for CLUSTER BY clause
    # =========================================================================
    if table_size_gb < config.large_table_threshold_gb and table_size_gb > 10 and not is_liquid_clustered:
        issues.append(TableIssue(
            issue_id="NO_LIQUID_CLUSTERING",
            category=IssueCategory.NO_LIQUID_CLUSTERING.value,
            severity=IssueSeverity.MEDIUM.value,
            title="No Liquid Clustering Configured",
            details=f"Table size: {table_size_gb} GB. Table does not use Liquid Clustering. "
                   f"Liquid Clustering provides automatic data layout optimization without manual OPTIMIZE ZORDER.",
            current_value="Not enabled",
            threshold="Recommended for tables >10GB and <1TB",
            recommendation="Enable Liquid Clustering by altering the table with CLUSTER BY clause. "
                          "Choose columns commonly used in query filters.",
            sql_command=f"ALTER TABLE {full_table_name} CLUSTER BY (column1, column2);"
        ))
    
    # =========================================================================
    # ISSUE 7: Large Table Without ZORDER
    # =========================================================================
    # Logic:
    # - If table > 500GB and no ZORDER columns detected in history
    # =========================================================================
    if table_size_gb > config.zorder_recommend_threshold_gb and not zorder_columns and not is_liquid_clustered:
        issues.append(TableIssue(
            issue_id="NO_ZORDER",
            category=IssueCategory.NO_ZORDER.value,
            severity=IssueSeverity.MEDIUM.value,
            title="Large Table Without ZORDER",
            details=f"Table size: {table_size_gb} GB. No ZORDER optimization detected in table history. "
                   f"ZORDER colocates related data for faster filtered queries.",
            current_value="No ZORDER columns",
            threshold=f"Recommended for tables >{config.zorder_recommend_threshold_gb} GB",
            recommendation="Run OPTIMIZE with ZORDER BY clause on columns commonly used in query filters. "
                          "Or consider migrating to Liquid Clustering.",
            sql_command=f"OPTIMIZE {full_table_name} ZORDER BY (column1, column2);"
        ))
    
    # =========================================================================
    # ISSUE 8: Excess Partitions
    # =========================================================================
    # Logic:
    # - Critical if partition count > 5000
    # - Warning if partition count > 4000 (80% of limit)
    # =========================================================================
    partition_count = partition_analysis.get("partition_count", 0)
    if partition_count > config.max_partition_count:
        issues.append(TableIssue(
            issue_id="EXCESS_PARTITIONS",
            category=IssueCategory.EXCESS_PARTITIONS.value,
            severity=IssueSeverity.CRITICAL.value,
            title="Excessive Partition Count",
            details=f"Partition count: {partition_count:,} exceeds maximum recommended ({config.max_partition_count:,}). "
                   f"Partition columns: {', '.join(partition_columns)}. "
                   f"Excessive partitions cause metadata overhead and slow query planning.",
            current_value=f"{partition_count:,} partitions",
            threshold=f"<{config.max_partition_count:,} partitions",
            recommendation="Re-evaluate partitioning strategy. Consider coarser granularity (month instead of day) "
                          "or fewer partition columns.",
            sql_command=None
        ))
    elif partition_count > config.max_partition_count * 0.8:
        issues.append(TableIssue(
            issue_id="HIGH_PARTITION_COUNT",
            category=IssueCategory.EXCESS_PARTITIONS.value,
            severity=IssueSeverity.MEDIUM.value,
            title="High Partition Count",
            details=f"Partition count: {partition_count:,} is approaching maximum ({config.max_partition_count:,}). "
                   f"Partition columns: {', '.join(partition_columns)}.",
            current_value=f"{partition_count:,} partitions",
            threshold=f"<{config.max_partition_count:,} partitions",
            recommendation="Monitor partition growth. Plan for re-partitioning if growth continues.",
            sql_command=None
        ))
    
    # =========================================================================
    # ISSUE 9: Partition Skew
    # =========================================================================
    # Logic:
    # - Same severity logic as file skew
    # =========================================================================
    if partition_analysis.get("has_partition_skew"):
        partition_skew_ratio = partition_analysis.get("partition_skew_ratio", 0)
        partition_skew_severity = partition_analysis.get("partition_skew_severity", IssueSeverity.MEDIUM.value)
        issues.append(TableIssue(
            issue_id=f"PARTITION_SKEW_{partition_skew_severity.upper()}",
            category=IssueCategory.PARTITION_SKEW.value,
            severity=partition_skew_severity,
            title=f"Partition Size Skew ({partition_skew_severity.capitalize()})",
            details=f"Partition skew ratio: {partition_skew_ratio}x (largest/median). "
                   f"Largest partition: {partition_analysis.get('largest_partition_size_mb', 0)} MB, "
                   f"Median partition: {partition_analysis.get('median_partition_size_mb', 0)} MB, "
                   f"Smallest partition: {partition_analysis.get('smallest_partition_size_mb', 0)} MB.",
            current_value=f"{partition_skew_ratio}x skew ratio",
            threshold=f"<{config.partition_skew_threshold}x",
            recommendation="Investigate partition key distribution. Consider using a different partition column "
                          "or transforming the partition key for more even distribution.",
            sql_command=None
        ))
    
    # Sort issues by severity
    severity_order = {
        IssueSeverity.CRITICAL.value: 0,
        IssueSeverity.HIGH.value: 1,
        IssueSeverity.MEDIUM.value: 2,
        IssueSeverity.LOW.value: 3,
        IssueSeverity.NONE.value: 4
    }
    issues.sort(key=lambda i: severity_order.get(i.severity, 5))
    
    return issues


def compute_health_status(issues: List[TableIssue]) -> str:
    """
    Compute overall health status based on detected issues.
    
    Logic:
    - CRITICAL: Any critical severity issue
    - WARNING: Any high severity issue
    - NEEDS_ATTENTION: Any medium severity issue
    - HEALTHY: Only low/no severity issues or no issues
    
    Args:
        issues: List of detected issues
        
    Returns:
        Health status string
    """
    if not issues:
        return HealthStatus.HEALTHY.value
    
    severities = [i.severity for i in issues]
    
    if IssueSeverity.CRITICAL.value in severities:
        return HealthStatus.CRITICAL.value
    elif IssueSeverity.HIGH.value in severities:
        return HealthStatus.WARNING.value
    elif IssueSeverity.MEDIUM.value in severities:
        return HealthStatus.NEEDS_ATTENTION.value
    else:
        return HealthStatus.HEALTHY.value

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Main Analysis Function

# COMMAND ----------

# DBTITLE 1,Main Table Analysis Function
@dataclass
class TableAnalysisResult:
    """Complete analysis result for a single table."""
    # Basic info
    catalog: str
    schema: str
    table: str
    full_name: str
    
    # Size metrics
    table_size_gb: float
    num_files: int
    avg_file_size_mb: float
    
    # Small files metrics
    small_files_pct: float
    severe_small_files_pct: float
    
    # Skew metrics
    file_skew_ratio: float
    file_skew_severity: str
    
    # Partition info
    is_partitioned: bool
    partition_count: int
    partition_columns: List[str]
    partition_skew_ratio: float
    partition_skew_severity: str
    
    # Clustering info
    is_liquid_clustered: bool
    clustering_columns: List[str]
    zorder_columns: List[str]
    
    # Maintenance status
    last_optimize: Optional[str]
    last_optimize_days_ago: Optional[int]
    optimize_status: str
    last_vacuum: Optional[str]
    last_vacuum_days_ago: Optional[int]
    vacuum_status: str
    
    # Health status
    health_status: str
    issue_count: int
    critical_issues: int
    high_issues: int
    medium_issues: int
    low_issues: int
    
    # Detailed results
    issues: List[TableIssue]
    sql_commands: List[str]
    analysis_timestamp: str
    
    # Raw data (for detailed analysis)
    metadata: Dict[str, Any]
    file_layout: Dict[str, Any]
    partition_analysis: Dict[str, Any]
    history_analysis: Dict[str, Any]
    optimize_info: Dict[str, Any]
    growth_analysis: Dict[str, Any]
    file_skew: Dict[str, Any]
    liquid_clustering: Dict[str, Any]
    
    error: Optional[str] = None


def analyze_table(
    catalog: str, 
    schema: str, 
    table: str, 
    config: AnalyzerConfig
) -> TableAnalysisResult:
    """
    Perform comprehensive analysis on a single Delta table.
    
    This is the main entry point for table analysis. It orchestrates all
    analysis functions and produces a complete result.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        config: Analyzer configuration
        
    Returns:
        TableAnalysisResult with complete analysis
    """
    full_name = f"{catalog}.{schema}.{table}"
    analysis_timestamp = datetime.now().isoformat()
    
    logger.info(f"Starting analysis of {full_name}")
    
    # Initialize with defaults
    default_result = TableAnalysisResult(
        catalog=catalog,
        schema=schema,
        table=table,
        full_name=full_name,
        table_size_gb=0.0,
        num_files=0,
        avg_file_size_mb=0.0,
        small_files_pct=0.0,
        severe_small_files_pct=0.0,
        file_skew_ratio=0.0,
        file_skew_severity=IssueSeverity.NONE.value,
        is_partitioned=False,
        partition_count=0,
        partition_columns=[],
        partition_skew_ratio=0.0,
        partition_skew_severity=IssueSeverity.NONE.value,
        is_liquid_clustered=False,
        clustering_columns=[],
        zorder_columns=[],
        last_optimize=None,
        last_optimize_days_ago=None,
        optimize_status="Never",
        last_vacuum=None,
        last_vacuum_days_ago=None,
        vacuum_status="Never",
        health_status=HealthStatus.ERROR.value,
        issue_count=0,
        critical_issues=0,
        high_issues=0,
        medium_issues=0,
        low_issues=0,
        issues=[],
        sql_commands=[],
        analysis_timestamp=analysis_timestamp,
        metadata={},
        file_layout={},
        partition_analysis={},
        history_analysis={},
        optimize_info={},
        growth_analysis={},
        file_skew={},
        liquid_clustering={}
    )
    
    try:
        # Step 1: Collect metadata
        logger.info(f"[1/8] Collecting metadata for {full_name}")
        metadata = collect_table_metadata(catalog, schema, table)
        default_result.metadata = metadata
        
        if not metadata.get("is_valid"):
            default_result.error = metadata.get("error", "Invalid table or not a Delta table")
            logger.warning(f"Table {full_name} is not valid: {default_result.error}")
            return default_result
        
        default_result.table_size_gb = metadata.get("table_size_gb", 0)
        default_result.num_files = metadata.get("num_files", 0)
        default_result.partition_columns = metadata.get("partition_columns", [])
        default_result.is_partitioned = bool(default_result.partition_columns)
        
        # Step 2: Detect Liquid Clustering
        logger.info(f"[2/8] Checking Liquid Clustering for {full_name}")
        liquid_clustering = detect_liquid_clustering(catalog, schema, table)
        default_result.liquid_clustering = liquid_clustering
        default_result.is_liquid_clustered = liquid_clustering.get("is_liquid_clustered", False)
        default_result.clustering_columns = liquid_clustering.get("clustering_columns", [])
        
        # Step 3: Analyze file layout
        logger.info(f"[3/8] Analyzing file layout for {full_name}")
        table_path = metadata.get("location")
        file_layout = {}
        file_skew = {}
        
        if table_path:
            file_layout = analyze_file_layout(table_path, config)
            default_result.file_layout = file_layout
            default_result.avg_file_size_mb = file_layout.get("avg_file_size_mb", 0)
            default_result.small_files_pct = file_layout.get("small_files_pct", 0)
            default_result.severe_small_files_pct = file_layout.get("severe_small_files_pct", 0)
            
            # Detect file skew
            if file_layout.get("file_sizes"):
                file_skew = detect_file_skew(file_layout["file_sizes"], config)
                default_result.file_skew = file_skew
                default_result.file_skew_ratio = file_skew.get("file_skew_ratio", 0)
                default_result.file_skew_severity = file_skew.get("skew_severity", IssueSeverity.NONE.value)
        
        # Step 4: Analyze partitions
        logger.info(f"[4/8] Analyzing partitions for {full_name}")
        partition_analysis = analyze_partitions(
            catalog, schema, table, 
            default_result.partition_columns, config
        )
        default_result.partition_analysis = partition_analysis
        default_result.partition_count = partition_analysis.get("partition_count", 0)
        default_result.partition_skew_ratio = partition_analysis.get("partition_skew_ratio", 0)
        default_result.partition_skew_severity = partition_analysis.get("partition_skew_severity", IssueSeverity.NONE.value)
        
        # Step 5: Analyze Delta history
        logger.info(f"[5/8] Analyzing Delta history for {full_name}")
        history_analysis = analyze_delta_history(catalog, schema, table, config)
        default_result.history_analysis = history_analysis
        default_result.last_optimize = history_analysis.get("last_optimize_timestamp")
        default_result.last_optimize_days_ago = history_analysis.get("days_since_last_optimize")
        default_result.optimize_status = history_analysis.get("optimize_status", "Never")
        default_result.last_vacuum = history_analysis.get("last_vacuum_timestamp")
        default_result.last_vacuum_days_ago = history_analysis.get("days_since_last_vacuum")
        default_result.vacuum_status = history_analysis.get("vacuum_status", "Never")
        
        # Step 6: Detect OPTIMIZE operations
        logger.info(f"[6/8] Detecting OPTIMIZE operations for {full_name}")
        optimize_info = detect_optimize_operations(history_analysis.get("history_df"))
        default_result.optimize_info = optimize_info
        default_result.zorder_columns = optimize_info.get("zorder_columns", [])
        
        # Step 7: Analyze growth
        logger.info(f"[7/8] Analyzing growth for {full_name}")
        growth_analysis = analyze_table_growth(
            history_analysis.get("history_df"),
            metadata.get("table_size_bytes", 0)
        )
        default_result.growth_analysis = growth_analysis
        
        # Step 8: Detect all issues
        logger.info(f"[8/8] Detecting issues for {full_name}")
        issues = detect_all_issues(
            full_name, metadata, file_layout, file_skew,
            partition_analysis, history_analysis, optimize_info,
            liquid_clustering, config
        )
        default_result.issues = issues
        default_result.issue_count = len(issues)
        default_result.critical_issues = sum(1 for i in issues if i.severity == IssueSeverity.CRITICAL.value)
        default_result.high_issues = sum(1 for i in issues if i.severity == IssueSeverity.HIGH.value)
        default_result.medium_issues = sum(1 for i in issues if i.severity == IssueSeverity.MEDIUM.value)
        default_result.low_issues = sum(1 for i in issues if i.severity == IssueSeverity.LOW.value)
        
        # Compute health status
        default_result.health_status = compute_health_status(issues)
        
        # Collect SQL commands
        default_result.sql_commands = [i.sql_command for i in issues if i.sql_command]
        
        logger.info(f"Analysis complete for {full_name}: {len(issues)} issues, status: {default_result.health_status}")
        
    except Exception as e:
        default_result.error = str(e)
        logger.error(f"Error analyzing {full_name}: {e}")
    
    return default_result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Report Output

# COMMAND ----------

# DBTITLE 1,Report Generation Functions
def create_report_dataframe(results: List[TableAnalysisResult]) -> DataFrame:
    """
    Create a Spark DataFrame from analysis results with clear issue columns.
    
    Args:
        results: List of TableAnalysisResult objects
        
    Returns:
        Spark DataFrame with analysis results
    """
    # Define schema with clear columns
    schema = StructType([
        StructField("catalog", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("table_size_gb", DoubleType(), True),
        StructField("num_files", LongType(), True),
        
        # Small Files columns
        StructField("avg_file_size_mb", DoubleType(), True),
        StructField("small_files_pct", DoubleType(), True),
        StructField("severe_small_files_pct", DoubleType(), True),
        
        # Skew columns
        StructField("file_skew_ratio", DoubleType(), True),
        StructField("file_skew_severity", StringType(), True),
        
        # Partition columns
        StructField("is_partitioned", BooleanType(), True),
        StructField("partition_count", IntegerType(), True),
        StructField("partition_columns", StringType(), True),
        StructField("partition_skew_ratio", DoubleType(), True),
        
        # Clustering columns
        StructField("is_liquid_clustered", BooleanType(), True),
        StructField("clustering_columns", StringType(), True),
        StructField("zorder_columns", StringType(), True),
        
        # Maintenance columns
        StructField("last_optimize", StringType(), True),
        StructField("last_optimize_days_ago", IntegerType(), True),
        StructField("optimize_status", StringType(), True),
        StructField("last_vacuum", StringType(), True),
        StructField("last_vacuum_days_ago", IntegerType(), True),
        StructField("vacuum_status", StringType(), True),
        
        # Health status columns
        StructField("health_status", StringType(), True),
        StructField("issue_count", IntegerType(), True),
        StructField("critical_issues", IntegerType(), True),
        StructField("high_issues", IntegerType(), True),
        StructField("medium_issues", IntegerType(), True),
        StructField("low_issues", IntegerType(), True),
        
        # Issue details
        StructField("issues_summary", StringType(), True),
        StructField("sql_commands", StringType(), True),
        
        # Metadata
        StructField("analysis_timestamp", StringType(), True),
        StructField("error", StringType(), True)
    ])
    
    # Convert results to rows
    rows = []
    for r in results:
        # Create issues summary
        issues_summary = []
        for issue in r.issues:
            issues_summary.append({
                "id": issue.issue_id,
                "category": issue.category,
                "severity": issue.severity,
                "title": issue.title,
                "details": issue.details
            })
        
        rows.append((
            r.catalog,
            r.schema,
            r.table,
            float(r.table_size_gb),
            int(r.num_files),
            
            # Small files
            float(r.avg_file_size_mb),
            float(r.small_files_pct),
            float(r.severe_small_files_pct),
            
            # Skew
            float(r.file_skew_ratio),
            r.file_skew_severity,
            
            # Partitions
            r.is_partitioned,
            int(r.partition_count),
            json.dumps(r.partition_columns) if r.partition_columns else "[]",
            float(r.partition_skew_ratio),
            
            # Clustering
            r.is_liquid_clustered,
            json.dumps(r.clustering_columns) if r.clustering_columns else "[]",
            json.dumps(r.zorder_columns) if r.zorder_columns else "[]",
            
            # Maintenance
            r.last_optimize,
            r.last_optimize_days_ago,
            r.optimize_status,
            r.last_vacuum,
            r.last_vacuum_days_ago,
            r.vacuum_status,
            
            # Health
            r.health_status,
            r.issue_count,
            r.critical_issues,
            r.high_issues,
            r.medium_issues,
            r.low_issues,
            
            # Details
            json.dumps(issues_summary) if issues_summary else "[]",
            json.dumps(r.sql_commands) if r.sql_commands else "[]",
            
            # Metadata
            r.analysis_timestamp,
            r.error
        ))
    
    return spark.createDataFrame(rows, schema)


def create_issues_dataframe(results: List[TableAnalysisResult]) -> DataFrame:
    """
    Create a DataFrame with one row per issue for detailed analysis.
    
    Args:
        results: List of TableAnalysisResult objects
        
    Returns:
        Spark DataFrame with issues
    """
    schema = StructType([
        StructField("catalog", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("issue_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("title", StringType(), True),
        StructField("details", StringType(), True),
        StructField("current_value", StringType(), True),
        StructField("threshold", StringType(), True),
        StructField("recommendation", StringType(), True),
        StructField("sql_command", StringType(), True),
        StructField("analysis_timestamp", StringType(), True)
    ])
    
    rows = []
    for r in results:
        for issue in r.issues:
            rows.append((
                r.catalog,
                r.schema,
                r.table,
                issue.issue_id,
                issue.category,
                issue.severity,
                issue.title,
                issue.details,
                issue.current_value,
                issue.threshold,
                issue.recommendation,
                issue.sql_command,
                r.analysis_timestamp
            ))
    
    if not rows:
        return spark.createDataFrame([], schema)
    
    return spark.createDataFrame(rows, schema)


def display_summary_report(results: List[TableAnalysisResult]) -> None:
    """
    Display a formatted summary report of analysis results.
    
    Args:
        results: List of TableAnalysisResult objects
    """
    print("\n" + "=" * 80)
    print("DELTA TABLE LAYOUT ANALYSIS REPORT")
    print("=" * 80)
    print(f"Analysis Timestamp: {datetime.now().isoformat()}")
    print(f"Tables Analyzed: {len(results)}")
    
    # Summary counts
    healthy = sum(1 for r in results if r.health_status == HealthStatus.HEALTHY.value)
    needs_attention = sum(1 for r in results if r.health_status == HealthStatus.NEEDS_ATTENTION.value)
    warning = sum(1 for r in results if r.health_status == HealthStatus.WARNING.value)
    critical = sum(1 for r in results if r.health_status == HealthStatus.CRITICAL.value)
    
    print(f"\nHealth Summary:")
    print(f"  ✅ Healthy:         {healthy}")
    print(f"  ⚠️  Needs Attention: {needs_attention}")
    print(f"  🟠 Warning:         {warning}")
    print(f"  🔴 Critical:        {critical}")
    print("=" * 80 + "\n")
    
    for r in results:
        print(f"\n{'─' * 70}")
        
        # Health status indicator
        status_icons = {
            HealthStatus.HEALTHY.value: "✅",
            HealthStatus.NEEDS_ATTENTION.value: "⚠️",
            HealthStatus.WARNING.value: "🟠",
            HealthStatus.CRITICAL.value: "🔴",
            HealthStatus.ERROR.value: "❌"
        }
        status_icon = status_icons.get(r.health_status, "❓")
        
        print(f"{status_icon} TABLE: {r.full_name}")
        print(f"   Health Status: {r.health_status.upper()}")
        print(f"{'─' * 70}")
        
        if r.error:
            print(f"❌ Error: {r.error}")
            continue
        
        # Storage metrics
        print(f"\n📁 STORAGE METRICS:")
        print(f"   • Table Size:           {r.table_size_gb:.2f} GB")
        print(f"   • Number of Files:      {r.num_files:,}")
        print(f"   • Avg File Size:        {r.avg_file_size_mb:.2f} MB")
        print(f"   • Small Files (<128MB): {r.small_files_pct:.1f}%")
        print(f"   • Severe Small (<32MB): {r.severe_small_files_pct:.1f}%")
        
        # Skew info
        print(f"\n⚖️  SKEW ANALYSIS:")
        print(f"   • File Skew Ratio:      {r.file_skew_ratio:.1f}x")
        print(f"   • File Skew Severity:   {r.file_skew_severity}")
        if r.is_partitioned:
            print(f"   • Partition Skew:       {r.partition_skew_ratio:.1f}x ({r.partition_skew_severity})")
        
        # Partition info
        print(f"\n📊 PARTITIONING:")
        print(f"   • Is Partitioned:       {r.is_partitioned}")
        if r.is_partitioned:
            print(f"   • Partition Columns:    {', '.join(r.partition_columns)}")
            print(f"   • Partition Count:      {r.partition_count:,}")
        
        # Clustering info
        print(f"\n🔗 CLUSTERING:")
        print(f"   • Liquid Clustering:    {r.is_liquid_clustered}")
        if r.is_liquid_clustered:
            print(f"   • Clustering Columns:   {', '.join(r.clustering_columns)}")
        if r.zorder_columns:
            print(f"   • ZORDER Columns:       {', '.join(r.zorder_columns)}")
        
        # Maintenance status
        print(f"\n🔧 MAINTENANCE STATUS:")
        print(f"   • Last OPTIMIZE:        {r.optimize_status}")
        print(f"   • Last VACUUM:          {r.vacuum_status}")
        
        # Issues
        if r.issues:
            print(f"\n⚠️  ISSUES DETECTED ({r.issue_count}):")
            print(f"   • Critical: {r.critical_issues}, High: {r.high_issues}, Medium: {r.medium_issues}, Low: {r.low_issues}")
            print()
            
            for issue in r.issues:
                severity_icons = {
                    IssueSeverity.CRITICAL.value: "🔴",
                    IssueSeverity.HIGH.value: "🟠",
                    IssueSeverity.MEDIUM.value: "🟡",
                    IssueSeverity.LOW.value: "🟢"
                }
                icon = severity_icons.get(issue.severity, "⚪")
                
                print(f"   {icon} [{issue.severity.upper()}] {issue.title}")
                print(f"      Category: {issue.category}")
                print(f"      Current:  {issue.current_value}")
                print(f"      Expected: {issue.threshold}")
                print(f"      Action:   {issue.recommendation}")
                if issue.sql_command:
                    print(f"      SQL:      {issue.sql_command}")
                print()
        else:
            print(f"\n✅ NO ISSUES - Table is well optimized!")
    
    print("\n" + "=" * 80)
    print("END OF REPORT")
    print("=" * 80 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Schema Scan Function

# COMMAND ----------

# DBTITLE 1,Schema Scan Function
def scan_schema_tables(
    catalog: str, 
    schema: str, 
    config: AnalyzerConfig
) -> List[TableAnalysisResult]:
    """
    Scan and analyze all tables in a schema.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        config: Analyzer configuration
        
    Returns:
        List of TableAnalysisResult objects
    """
    logger.info(f"Scanning all tables in {catalog}.{schema}")
    
    results = []
    tables = list_schema_tables(catalog, schema)
    
    logger.info(f"Found {len(tables)} tables to analyze")
    
    for i, table_info in enumerate(tables, 1):
        table_name = table_info["table"]
        logger.info(f"Analyzing table {i}/{len(tables)}: {table_name}")
        
        # Check if it's a Delta table before analyzing
        if not is_delta_table(catalog, schema, table_name):
            logger.info(f"Skipping {table_name} - not a Delta table")
            continue
        
        result = analyze_table(catalog, schema, table_name, config)
        results.append(result)
    
    logger.info(f"Schema scan complete: analyzed {len(results)} Delta tables")
    return results


def scan_catalog_tables(catalog: str, config: AnalyzerConfig) -> List[TableAnalysisResult]:
    """
    Scan and analyze all tables in a catalog.
    
    Args:
        catalog: Catalog name
        config: Analyzer configuration
        
    Returns:
        List of TableAnalysisResult objects
    """
    logger.info(f"Scanning all tables in catalog {catalog}")
    
    results = []
    tables = list_catalog_tables(catalog)
    
    logger.info(f"Found {len(tables)} tables to analyze")
    
    for i, table_info in enumerate(tables, 1):
        cat = table_info["catalog"]
        sch = table_info["schema"]
        tab = table_info["table"]
        
        logger.info(f"Analyzing table {i}/{len(tables)}: {cat}.{sch}.{tab}")
        
        if not is_delta_table(cat, sch, tab):
            logger.info(f"Skipping {cat}.{sch}.{tab} - not a Delta table")
            continue
        
        result = analyze_table(cat, sch, tab, config)
        results.append(result)
    
    logger.info(f"Catalog scan complete: analyzed {len(results)} Delta tables")
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Persist Results

# COMMAND ----------

# DBTITLE 1,Persist Results to Delta Table
def persist_results(
    results: List[TableAnalysisResult], 
    config: AnalyzerConfig
) -> bool:
    """
    Persist analysis results to a Delta table.
    
    Args:
        results: List of TableAnalysisResult objects
        config: Analyzer configuration
        
    Returns:
        True if successful, False otherwise
    """
    if not config.persist_results:
        logger.info("Result persistence is disabled")
        return False
    
    output_table = config.get_output_table_name()
    logger.info(f"Persisting results to {output_table}")
    
    try:
        # Create report DataFrame
        report_df = create_report_dataframe(results)
        
        # Create output schema/database if needed
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.output_catalog}.{config.output_schema}")
        
        # Write to Delta table (append mode to keep history)
        report_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(output_table)
        
        logger.info(f"Successfully persisted {len(results)} results to {output_table}")
        return True
        
    except Exception as e:
        logger.error(f"Error persisting results: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Main Execution

# COMMAND ----------

# DBTITLE 1,Execute Analysis
def run_analysis(config: AnalyzerConfig) -> List[TableAnalysisResult]:
    """
    Main entry point to run the Delta Layout Analyzer.
    
    Args:
        config: Analyzer configuration
        
    Returns:
        List of TableAnalysisResult objects
    """
    print("\n" + "=" * 60)
    print("🚀 DELTA LAYOUT ANALYZER - Starting Analysis")
    print("=" * 60)
    print(f"Mode:      {config.scan_mode}")
    print(f"Catalog:   {config.catalog}")
    print(f"Schema:    {config.schema}")
    if config.scan_mode == "single_table":
        print(f"Table:     {config.table}")
    print("=" * 60 + "\n")
    
    results = []
    
    if config.scan_mode == "single_table":
        # Analyze single table
        result = analyze_table(config.catalog, config.schema, config.table, config)
        results.append(result)
        
    elif config.scan_mode == "schema":
        # Analyze all tables in schema
        results = scan_schema_tables(config.catalog, config.schema, config)
        
    elif config.scan_mode == "catalog":
        # Analyze all tables in catalog
        results = scan_catalog_tables(config.catalog, config)
    
    # Display summary report
    display_summary_report(results)
    
    # Persist results if enabled
    if config.persist_results:
        persist_results(results, config)
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the Analyzer
# MAGIC Execute the cell below to start the analysis.

# COMMAND ----------

# DBTITLE 1,Run Analysis
# Execute the analysis with current configuration
results = run_analysis(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. Display Results as DataFrame

# COMMAND ----------

# DBTITLE 1,Results Summary DataFrame
# Create and display results as a DataFrame
if results:
    report_df = create_report_dataframe(results)
    display(report_df)
else:
    print("No results to display")

# COMMAND ----------

# DBTITLE 1,Issues Detail DataFrame
# Display detailed issues
if results:
    issues_df = create_issues_dataframe(results)
    if issues_df.count() > 0:
        display(issues_df)
    else:
        print("No issues detected - all tables are healthy!")
else:
    print("No results to display")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 19. Optimization SQL Commands

# COMMAND ----------

# DBTITLE 1,Generated SQL Commands
# Display all generated optimization SQL commands
print("=" * 60)
print("GENERATED OPTIMIZATION SQL COMMANDS")
print("=" * 60 + "\n")

for r in results:
    if r.sql_commands:
        print(f"-- Table: {r.full_name}")
        print(f"-- Health Status: {r.health_status}")
        for cmd in r.sql_commands:
            print(cmd)
        print()

if not any(r.sql_commands for r in results):
    print("No optimization commands generated - tables are well optimized!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 20. Query Persisted Results (Optional)

# COMMAND ----------

# DBTITLE 1,Query Historical Analysis Results
# Query historical analysis results from the output table
if config.persist_results:
    try:
        historical_df = spark.sql(f"""
            SELECT 
                catalog,
                schema,
                table,
                table_size_gb,
                num_files,
                avg_file_size_mb,
                small_files_pct,
                file_skew_severity,
                is_partitioned,
                is_liquid_clustered,
                optimize_status,
                vacuum_status,
                health_status,
                issue_count,
                analysis_timestamp
            FROM {config.get_output_table_name()}
            ORDER BY analysis_timestamp DESC
            LIMIT 100
        """)
        display(historical_df)
    except Exception as e:
        print(f"No historical data available yet: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Appendix: Usage Examples
# MAGIC 
# MAGIC ### Example 1: Analyze a Single Table
# MAGIC ```python
# MAGIC config = AnalyzerConfig(
# MAGIC     catalog="my_catalog",
# MAGIC     schema="my_schema",
# MAGIC     table="my_table",
# MAGIC     scan_mode="single_table"
# MAGIC )
# MAGIC results = run_analysis(config)
# MAGIC ```
# MAGIC 
# MAGIC ### Example 2: Scan All Tables in a Schema
# MAGIC ```python
# MAGIC config = AnalyzerConfig(
# MAGIC     catalog="my_catalog",
# MAGIC     schema="my_schema",
# MAGIC     scan_mode="schema"
# MAGIC )
# MAGIC results = run_analysis(config)
# MAGIC ```
# MAGIC 
# MAGIC ### Example 3: Scan Entire Catalog
# MAGIC ```python
# MAGIC config = AnalyzerConfig(
# MAGIC     catalog="my_catalog",
# MAGIC     scan_mode="catalog"
# MAGIC )
# MAGIC results = run_analysis(config)
# MAGIC ```
# MAGIC 
# MAGIC ### Example 4: Custom Thresholds
# MAGIC ```python
# MAGIC config = AnalyzerConfig(
# MAGIC     catalog="my_catalog",
# MAGIC     schema="my_schema",
# MAGIC     table="my_table",
# MAGIC     small_file_severe_mb=64,
# MAGIC     file_skew_threshold=5.0,
# MAGIC     optimize_stale_days=14,
# MAGIC     persist_results=True
# MAGIC )
# MAGIC results = run_analysis(config)
# MAGIC ```

# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table Layout Analyzer
# MAGIC 
# MAGIC ## Overview
# MAGIC This production-grade notebook analyzes Delta table storage layout and history to generate optimization recommendations.
# MAGIC 
# MAGIC **Features:**
# MAGIC - Analyzes file layout and detects small files
# MAGIC - Detects partition skew and file size skew
# MAGIC - Analyzes Delta history for OPTIMIZE, VACUUM, and MERGE operations
# MAGIC - Generates actionable optimization recommendations
# MAGIC - Supports Unity Catalog tables (managed and external)
# MAGIC - Scales to thousands of tables
# MAGIC 
# MAGIC **Author:** Databricks Platform Engineering  
# MAGIC **Version:** 1.0.0

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
    
    # Health check thresholds
    max_partition_count: int = 5000
    large_table_threshold_gb: int = 1000  # 1TB
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
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from functools import reduce
import json
import statistics

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, ArrayType, TimestampType, IntegerType
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
# MAGIC ## 5. File Layout Analysis

# COMMAND ----------

# DBTITLE 1,File Layout Analysis Functions
def analyze_file_layout(table_path: str, config: AnalyzerConfig) -> Dict[str, Any]:
    """
    Analyze file layout metrics for a Delta table using Delta log metadata.
    
    This function uses Delta Lake's internal metadata to analyze file sizes
    without scanning the actual data files, making it efficient for large tables.
    
    Args:
        table_path: Path to the Delta table
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
        "error": None
    }
    
    try:
        # Use Delta table's file metadata from the transaction log
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Get add files from the Delta log (metadata only, no data scan)
        files_df = delta_table.toDF()
        
        # Get file statistics from input_file_name() and metadata
        # This approach uses the Delta transaction log
        detail_df = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
        detail_row = detail_df.first()
        
        num_files = detail_row.numFiles if detail_row else 0
        total_size = detail_row.sizeInBytes if detail_row else 0
        
        if num_files == 0:
            return result
        
        # Calculate average file size
        avg_size_bytes = total_size / num_files if num_files > 0 else 0
        avg_size_mb = bytes_to_mb(avg_size_bytes)
        
        result.update({
            "total_files": num_files,
            "total_size_bytes": total_size,
            "avg_file_size_bytes": avg_size_bytes,
            "avg_file_size_mb": avg_size_mb,
        })
        
        # For detailed file analysis, we need to read the Delta log
        # Use _delta_log to get actual file sizes
        file_stats = get_file_stats_from_delta_log(table_path)
        
        if file_stats:
            file_sizes_mb = [bytes_to_mb(s) for s in file_stats]
            
            result.update({
                "file_sizes": file_stats,
                "median_file_size_mb": round(statistics.median(file_sizes_mb), 2) if file_sizes_mb else 0.0,
                "min_file_size_mb": round(min(file_sizes_mb), 2) if file_sizes_mb else 0.0,
                "max_file_size_mb": round(max(file_sizes_mb), 2) if file_sizes_mb else 0.0,
                "std_dev_mb": round(statistics.stdev(file_sizes_mb), 2) if len(file_sizes_mb) > 1 else 0.0,
            })
            
            # Categorize files by size
            severe_threshold = config.small_file_severe_mb * 1024 * 1024
            acceptable_threshold = config.small_file_acceptable_mb * 1024 * 1024
            optimal_min = config.optimal_file_size_min_mb * 1024 * 1024
            optimal_max = config.optimal_file_size_max_mb * 1024 * 1024
            
            severe_count = sum(1 for s in file_stats if s < severe_threshold)
            small_count = sum(1 for s in file_stats if s < acceptable_threshold)
            optimal_count = sum(1 for s in file_stats if optimal_min <= s <= optimal_max)
            
            result.update({
                "severe_small_files_count": severe_count,
                "severe_small_files_pct": round(100 * severe_count / len(file_stats), 2) if file_stats else 0.0,
                "small_files_count": small_count,
                "small_files_pct": round(100 * small_count / len(file_stats), 2) if file_stats else 0.0,
                "optimal_files_count": optimal_count,
                "optimal_files_pct": round(100 * optimal_count / len(file_stats), 2) if file_stats else 0.0,
            })
        
        logger.info(f"Analyzed file layout: {num_files} files, avg size: {result['avg_file_size_mb']} MB")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error analyzing file layout for {table_path}: {e}")
    
    return result


def get_file_stats_from_delta_log(table_path: str, sample_size: int = 10000) -> List[int]:
    """
    Extract file sizes from Delta transaction log.
    
    This reads the Delta log JSON files to get file sizes without scanning data.
    
    Args:
        table_path: Path to Delta table
        sample_size: Maximum number of files to sample for statistics
        
    Returns:
        List of file sizes in bytes
    """
    file_sizes = []
    
    try:
        delta_log_path = f"{table_path}/_delta_log"
        
        # Read the latest checkpoint or JSON files
        # Try to read add file statistics from the log
        log_df = spark.read.json(f"{delta_log_path}/*.json")
        
        if "add" in log_df.columns:
            add_files_df = log_df.select(
                F.col("add.path").alias("path"),
                F.col("add.size").alias("size")
            ).filter(F.col("path").isNotNull())
            
            # Sample if there are too many files
            if add_files_df.count() > sample_size:
                add_files_df = add_files_df.sample(fraction=sample_size / add_files_df.count())
            
            file_sizes = [row.size for row in add_files_df.collect() if row.size is not None]
            
    except Exception as e:
        logger.debug(f"Could not read Delta log for {table_path}: {e}")
        # Fall back to using DESCRIBE DETAIL average
    
    return file_sizes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Partition Analysis

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
        "partition_count": 0,
        "partition_columns": partition_columns,
        "largest_partition_size_mb": 0.0,
        "smallest_partition_size_mb": 0.0,
        "median_partition_size_mb": 0.0,
        "avg_partition_size_mb": 0.0,
        "partition_skew_ratio": 0.0,
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
    Get approximate partition sizes using file metadata.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        partition_columns: Partition column names
        sample_limit: Maximum number of partitions to analyze
        
    Returns:
        List of partition sizes in bytes
    """
    try:
        full_name = f"{catalog}.{schema}.{table}"
        detail_df = spark.sql(f"DESCRIBE DETAIL {full_name}")
        location = detail_df.first().location
        
        # Read Delta log to get file sizes per partition
        delta_log_path = f"{location}/_delta_log"
        
        try:
            log_df = spark.read.json(f"{delta_log_path}/*.json")
            
            if "add" in log_df.columns:
                add_df = log_df.select(
                    F.col("add.path").alias("path"),
                    F.col("add.size").alias("size"),
                    F.col("add.partitionValues").alias("partition_values")
                ).filter(F.col("path").isNotNull())
                
                # Group by partition and sum sizes
                partition_sizes_df = add_df.groupBy("partition_values").agg(
                    F.sum("size").alias("total_size")
                ).limit(sample_limit)
                
                return [row.total_size for row in partition_sizes_df.collect() if row.total_size]
        except Exception:
            pass
        
        # Fallback: estimate based on total size and partition count
        detail_row = detail_df.first()
        total_size = detail_row.sizeInBytes if detail_row else 0
        
        # Get partition count
        try:
            partitions_df = spark.sql(f"SHOW PARTITIONS {full_name}")
            partition_count = partitions_df.count()
            if partition_count > 0:
                avg_size = total_size / partition_count
                return [int(avg_size)] * min(partition_count, sample_limit)
        except Exception:
            pass
            
    except Exception as e:
        logger.debug(f"Error getting partition sizes: {e}")
    
    return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. File Size Skew Detection

# COMMAND ----------

# DBTITLE 1,File Size Skew Detection
def detect_file_skew(file_sizes: List[int], config: AnalyzerConfig) -> Dict[str, Any]:
    """
    Detect file size skew in a Delta table.
    
    File skew occurs when there's a large variance in file sizes,
    which can impact query performance.
    
    Args:
        file_sizes: List of file sizes in bytes
        config: Analyzer configuration
        
    Returns:
        Dictionary containing skew analysis results
    """
    result = {
        "file_skew_ratio": 0.0,
        "has_file_skew": False,
        "skew_severity": "none",
        "coefficient_of_variation": 0.0,
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
        
        # Determine severity
        if skew_ratio <= 3:
            result["skew_severity"] = "none"
        elif skew_ratio <= 5:
            result["skew_severity"] = "low"
        elif skew_ratio <= 10:
            result["skew_severity"] = "medium"
        else:
            result["skew_severity"] = "high"
        
        logger.info(f"File skew analysis: ratio={skew_ratio:.2f}, severity={result['skew_severity']}")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error detecting file skew: {e}")
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Delta History Analysis

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
        if last_timestamps.get("OPTIMIZE"):
            result["days_since_last_optimize"] = (now - last_timestamps["OPTIMIZE"]).days
        if last_timestamps.get("VACUUM END") or last_timestamps.get("VACUUM START"):
            vacuum_ts = last_timestamps.get("VACUUM END") or last_timestamps.get("VACUUM START")
            result["days_since_last_vacuum"] = (now - vacuum_ts).days
        
        logger.info(f"Analyzed history for {full_name}: {result['total_commits']} commits")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error analyzing history for {full_name}: {e}")
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. OPTIMIZE & ZORDER Detection

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
# MAGIC ## 10. Table Growth Analysis

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
# MAGIC ## 11. Additional Health Checks

# COMMAND ----------

# DBTITLE 1,Health Check Functions
@dataclass
class HealthCheckResult:
    """Result of a single health check."""
    check_name: str
    status: str  # "pass", "warn", "fail"
    message: str
    value: Any = None
    threshold: Any = None
    severity: str = "info"  # "info", "warning", "critical"


def run_health_checks(
    metadata: Dict[str, Any],
    file_layout: Dict[str, Any],
    partition_analysis: Dict[str, Any],
    history_analysis: Dict[str, Any],
    optimize_info: Dict[str, Any],
    config: AnalyzerConfig
) -> List[HealthCheckResult]:
    """
    Run comprehensive health checks on a Delta table.
    
    Args:
        metadata: Table metadata
        file_layout: File layout analysis results
        partition_analysis: Partition analysis results
        history_analysis: History analysis results
        optimize_info: OPTIMIZE detection results
        config: Analyzer configuration
        
    Returns:
        List of HealthCheckResult objects
    """
    checks = []
    
    # 1. Small Files Check
    avg_file_size_mb = file_layout.get("avg_file_size_mb", 0)
    if avg_file_size_mb > 0:
        if avg_file_size_mb < config.small_file_severe_mb:
            checks.append(HealthCheckResult(
                check_name="small_files",
                status="fail",
                message=f"Severe small files detected: avg size {avg_file_size_mb} MB < {config.small_file_severe_mb} MB threshold",
                value=avg_file_size_mb,
                threshold=config.small_file_severe_mb,
                severity="critical"
            ))
        elif avg_file_size_mb < config.small_file_acceptable_mb:
            checks.append(HealthCheckResult(
                check_name="small_files",
                status="warn",
                message=f"Small files detected: avg size {avg_file_size_mb} MB < {config.small_file_acceptable_mb} MB threshold",
                value=avg_file_size_mb,
                threshold=config.small_file_acceptable_mb,
                severity="warning"
            ))
        else:
            checks.append(HealthCheckResult(
                check_name="small_files",
                status="pass",
                message=f"File sizes acceptable: avg {avg_file_size_mb} MB",
                value=avg_file_size_mb,
                threshold=config.small_file_acceptable_mb,
                severity="info"
            ))
    
    # 2. Excess Partitions Check
    partition_count = partition_analysis.get("partition_count", 0)
    if partition_count > config.max_partition_count:
        checks.append(HealthCheckResult(
            check_name="excess_partitions",
            status="fail",
            message=f"Excess partitions: {partition_count} > {config.max_partition_count} threshold",
            value=partition_count,
            threshold=config.max_partition_count,
            severity="critical"
        ))
    elif partition_count > config.max_partition_count * 0.8:
        checks.append(HealthCheckResult(
            check_name="excess_partitions",
            status="warn",
            message=f"Approaching partition limit: {partition_count} (threshold: {config.max_partition_count})",
            value=partition_count,
            threshold=config.max_partition_count,
            severity="warning"
        ))
    elif partition_count > 0:
        checks.append(HealthCheckResult(
            check_name="partitions",
            status="pass",
            message=f"Partition count acceptable: {partition_count}",
            value=partition_count,
            threshold=config.max_partition_count,
            severity="info"
        ))
    
    # 3. Large Table Check
    table_size_gb = metadata.get("table_size_gb", 0)
    if table_size_gb > config.large_table_threshold_gb:
        checks.append(HealthCheckResult(
            check_name="large_table",
            status="warn",
            message=f"Large table: {table_size_gb} GB > {config.large_table_threshold_gb} GB",
            value=table_size_gb,
            threshold=config.large_table_threshold_gb,
            severity="warning"
        ))
    
    # 4. No Recent OPTIMIZE Check
    days_since_optimize = history_analysis.get("days_since_last_optimize")
    if days_since_optimize is None:
        checks.append(HealthCheckResult(
            check_name="no_optimize",
            status="fail",
            message="Table has never been optimized",
            value=None,
            threshold=config.optimize_stale_days,
            severity="critical"
        ))
    elif days_since_optimize > config.optimize_stale_days:
        checks.append(HealthCheckResult(
            check_name="stale_optimize",
            status="warn",
            message=f"OPTIMIZE is stale: {days_since_optimize} days since last run (threshold: {config.optimize_stale_days})",
            value=days_since_optimize,
            threshold=config.optimize_stale_days,
            severity="warning"
        ))
    else:
        checks.append(HealthCheckResult(
            check_name="optimize",
            status="pass",
            message=f"OPTIMIZE recent: {days_since_optimize} days ago",
            value=days_since_optimize,
            threshold=config.optimize_stale_days,
            severity="info"
        ))
    
    # 5. No Recent VACUUM Check
    days_since_vacuum = history_analysis.get("days_since_last_vacuum")
    if days_since_vacuum is None:
        checks.append(HealthCheckResult(
            check_name="no_vacuum",
            status="warn",
            message="Table has never been vacuumed",
            value=None,
            threshold=config.vacuum_stale_days,
            severity="warning"
        ))
    elif days_since_vacuum > config.vacuum_stale_days:
        checks.append(HealthCheckResult(
            check_name="stale_vacuum",
            status="warn",
            message=f"VACUUM is stale: {days_since_vacuum} days since last run (threshold: {config.vacuum_stale_days})",
            value=days_since_vacuum,
            threshold=config.vacuum_stale_days,
            severity="warning"
        ))
    else:
        checks.append(HealthCheckResult(
            check_name="vacuum",
            status="pass",
            message=f"VACUUM recent: {days_since_vacuum} days ago",
            value=days_since_vacuum,
            threshold=config.vacuum_stale_days,
            severity="info"
        ))
    
    # 6. File Skew Check
    file_skew_ratio = file_layout.get("file_skew_ratio", 0) if "file_skew_ratio" in file_layout else 0
    if file_skew_ratio > config.file_skew_threshold:
        checks.append(HealthCheckResult(
            check_name="file_skew",
            status="warn",
            message=f"File size skew detected: ratio {file_skew_ratio} > {config.file_skew_threshold}",
            value=file_skew_ratio,
            threshold=config.file_skew_threshold,
            severity="warning"
        ))
    
    # 7. Partition Skew Check
    if partition_analysis.get("has_partition_skew"):
        checks.append(HealthCheckResult(
            check_name="partition_skew",
            status="warn",
            message=f"Partition skew detected: ratio {partition_analysis.get('partition_skew_ratio')}",
            value=partition_analysis.get("partition_skew_ratio"),
            threshold=config.partition_skew_threshold,
            severity="warning"
        ))
    
    # 8. Large Table Without ZORDER Check
    if table_size_gb > config.zorder_recommend_threshold_gb:
        zorder_cols = optimize_info.get("zorder_columns", [])
        if not zorder_cols:
            checks.append(HealthCheckResult(
                check_name="no_zorder",
                status="warn",
                message=f"Large table ({table_size_gb} GB) without ZORDER optimization",
                value=table_size_gb,
                threshold=config.zorder_recommend_threshold_gb,
                severity="warning"
            ))
    
    return checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Recommendation Engine

# COMMAND ----------

# DBTITLE 1,Recommendation Engine
@dataclass
class Recommendation:
    """A single optimization recommendation."""
    category: str
    priority: str  # "critical", "high", "medium", "low"
    title: str
    description: str
    sql_command: Optional[str] = None
    impact: str = ""


def generate_recommendations(
    full_table_name: str,
    metadata: Dict[str, Any],
    file_layout: Dict[str, Any],
    partition_analysis: Dict[str, Any],
    file_skew: Dict[str, Any],
    history_analysis: Dict[str, Any],
    optimize_info: Dict[str, Any],
    health_checks: List[HealthCheckResult],
    config: AnalyzerConfig
) -> List[Recommendation]:
    """
    Generate optimization recommendations based on analysis results.
    
    This function evaluates all metrics and produces actionable recommendations
    with SQL commands where applicable.
    
    Args:
        full_table_name: Fully qualified table name
        metadata: Table metadata
        file_layout: File layout analysis
        partition_analysis: Partition analysis
        file_skew: File skew analysis
        history_analysis: History analysis
        optimize_info: OPTIMIZE detection results
        health_checks: Health check results
        config: Analyzer configuration
        
    Returns:
        List of Recommendation objects
    """
    recommendations = []
    
    table_size_gb = metadata.get("table_size_gb", 0)
    avg_file_size_mb = file_layout.get("avg_file_size_mb", 0)
    partition_columns = metadata.get("partition_columns", [])
    zorder_columns = optimize_info.get("zorder_columns", [])
    
    # 1. Small Files Recommendation
    if avg_file_size_mb < config.small_file_severe_mb and avg_file_size_mb > 0:
        recommendations.append(Recommendation(
            category="file_compaction",
            priority="critical",
            title="Run OPTIMIZE to compact small files",
            description=f"Average file size ({avg_file_size_mb} MB) is below the severe threshold ({config.small_file_severe_mb} MB). "
                       f"Small files significantly impact query performance.",
            sql_command=f"OPTIMIZE {full_table_name};",
            impact="Improved query performance, reduced storage overhead"
        ))
    elif avg_file_size_mb < config.small_file_acceptable_mb and avg_file_size_mb > 0:
        recommendations.append(Recommendation(
            category="file_compaction",
            priority="high",
            title="Consider running OPTIMIZE",
            description=f"Average file size ({avg_file_size_mb} MB) is below acceptable threshold ({config.small_file_acceptable_mb} MB).",
            sql_command=f"OPTIMIZE {full_table_name};",
            impact="Better query performance"
        ))
    
    # 2. File Skew Recommendation
    file_skew_ratio = file_skew.get("file_skew_ratio", 0)
    if file_skew_ratio > config.file_skew_threshold:
        recommendations.append(Recommendation(
            category="data_skew",
            priority="high",
            title="Investigate and fix file size skew",
            description=f"File size skew ratio ({file_skew_ratio:.1f}x) exceeds threshold ({config.file_skew_threshold}x). "
                       f"This may indicate uneven data distribution during writes.",
            sql_command=None,
            impact="More predictable query performance"
        ))
        recommendations.append(Recommendation(
            category="data_skew",
            priority="medium",
            title="Consider repartitioning before writes",
            description="Repartition data before writing to ensure even file sizes. "
                       "Use df.repartition() or coalesce() to control output file count.",
            sql_command=None,
            impact="Reduced file size variance"
        ))
    
    # 3. Large Table Without ZORDER Recommendation
    if table_size_gb > config.zorder_recommend_threshold_gb and not zorder_columns:
        # Try to suggest ZORDER columns based on common patterns
        suggested_cols = suggest_zorder_columns(metadata, history_analysis)
        
        zorder_clause = ""
        if suggested_cols:
            zorder_clause = f" ZORDER BY ({', '.join(suggested_cols)})"
            col_suggestion = f" Consider ZORDER BY ({', '.join(suggested_cols)}) based on common query patterns."
        else:
            col_suggestion = " Analyze query patterns to identify high-cardinality columns used in filters."
        
        recommendations.append(Recommendation(
            category="zorder",
            priority="high",
            title="Implement ZORDER optimization",
            description=f"Large table ({table_size_gb} GB) would benefit from ZORDER optimization.{col_suggestion}",
            sql_command=f"OPTIMIZE {full_table_name}{zorder_clause};" if zorder_clause else None,
            impact="Significantly improved query performance for filtered queries"
        ))
    
    # 4. Too Many Partitions Recommendation
    partition_count = partition_analysis.get("partition_count", 0)
    if partition_count > config.max_partition_count:
        recommendations.append(Recommendation(
            category="partitioning",
            priority="critical",
            title="Re-evaluate partition strategy",
            description=f"Table has {partition_count} partitions, exceeding the recommended maximum ({config.max_partition_count}). "
                       f"This can cause performance issues and metadata overhead.",
            sql_command=None,
            impact="Reduced metadata overhead, improved query planning"
        ))
        recommendations.append(Recommendation(
            category="partitioning",
            priority="high",
            title="Consider coarser partition granularity",
            description="If partitioning by date, consider using month or year instead of day. "
                       "For other columns, consider using partition transforms or fewer values.",
            sql_command=None,
            impact="Fewer partitions to manage"
        ))
    
    # 5. Stale OPTIMIZE Recommendation
    days_since_optimize = history_analysis.get("days_since_last_optimize")
    if days_since_optimize is None or days_since_optimize > config.optimize_stale_days:
        days_str = "never" if days_since_optimize is None else f"{days_since_optimize} days ago"
        
        optimize_cmd = f"OPTIMIZE {full_table_name}"
        if zorder_columns:
            optimize_cmd += f" ZORDER BY ({', '.join(zorder_columns)})"
        optimize_cmd += ";"
        
        recommendations.append(Recommendation(
            category="maintenance",
            priority="high" if days_since_optimize is None else "medium",
            title="Schedule regular OPTIMIZE jobs",
            description=f"Last OPTIMIZE was {days_str}. Regular optimization maintains query performance.",
            sql_command=optimize_cmd,
            impact="Maintained query performance over time"
        ))
    
    # 6. Stale VACUUM Recommendation
    days_since_vacuum = history_analysis.get("days_since_last_vacuum")
    if days_since_vacuum is None or days_since_vacuum > config.vacuum_stale_days:
        days_str = "never" if days_since_vacuum is None else f"{days_since_vacuum} days ago"
        recommendations.append(Recommendation(
            category="maintenance",
            priority="medium",
            title="Run VACUUM to reclaim storage",
            description=f"Last VACUUM was {days_str}. VACUUM removes old files and reclaims storage.",
            sql_command=f"VACUUM {full_table_name} RETAIN 168 HOURS;",
            impact="Reduced storage costs, cleaned up old files"
        ))
    
    # 7. Partition Skew Recommendation
    if partition_analysis.get("has_partition_skew"):
        recommendations.append(Recommendation(
            category="data_skew",
            priority="medium",
            title="Address partition skew",
            description=f"Partition skew detected (ratio: {partition_analysis.get('partition_skew_ratio'):.1f}). "
                       f"Some partitions are significantly larger than others.",
            sql_command=None,
            impact="More balanced query performance across partitions"
        ))
    
    # Sort recommendations by priority
    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    recommendations.sort(key=lambda r: priority_order.get(r.priority, 4))
    
    return recommendations


def suggest_zorder_columns(metadata: Dict[str, Any], history_analysis: Dict[str, Any]) -> List[str]:
    """
    Suggest ZORDER columns based on table patterns.
    
    This is a heuristic-based suggestion. For best results, analyze actual query patterns.
    
    Args:
        metadata: Table metadata
        history_analysis: History analysis results
        
    Returns:
        List of suggested column names
    """
    suggestions = []
    
    # Common ZORDER-worthy column patterns
    common_patterns = [
        "customer_id", "user_id", "account_id", "tenant_id",
        "transaction_id", "order_id", "event_id",
        "created_at", "updated_at", "event_time", "timestamp",
        "region", "country", "state"
    ]
    
    # This would ideally analyze table schema, but we keep it simple here
    # In production, you could query DESCRIBE TABLE to get column names
    
    return suggestions


def format_recommendations_summary(recommendations: List[Recommendation]) -> str:
    """Format recommendations as a readable summary."""
    if not recommendations:
        return "No recommendations - table is well optimized!"
    
    summary_parts = []
    for rec in recommendations:
        summary_parts.append(f"[{rec.priority.upper()}] {rec.title}")
    
    return " | ".join(summary_parts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Main Analysis Function

# COMMAND ----------

# DBTITLE 1,Main Table Analysis Function
@dataclass
class TableAnalysisResult:
    """Complete analysis result for a single table."""
    catalog: str
    schema: str
    table: str
    full_name: str
    table_size_gb: float
    num_files: int
    avg_file_size_mb: float
    partition_count: int
    partition_columns: List[str]
    skew_ratio: float
    last_optimize: Optional[str]
    last_vacuum: Optional[str]
    zorder_columns: List[str]
    recommendations: List[str]
    recommendations_detail: List[Recommendation]
    health_checks: List[HealthCheckResult]
    analysis_timestamp: str
    metadata: Dict[str, Any]
    file_layout: Dict[str, Any]
    partition_analysis: Dict[str, Any]
    history_analysis: Dict[str, Any]
    optimize_info: Dict[str, Any]
    growth_analysis: Dict[str, Any]
    file_skew: Dict[str, Any]
    sql_commands: List[str]
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
    
    # Initialize result with defaults
    result = TableAnalysisResult(
        catalog=catalog,
        schema=schema,
        table=table,
        full_name=full_name,
        table_size_gb=0.0,
        num_files=0,
        avg_file_size_mb=0.0,
        partition_count=0,
        partition_columns=[],
        skew_ratio=0.0,
        last_optimize=None,
        last_vacuum=None,
        zorder_columns=[],
        recommendations=[],
        recommendations_detail=[],
        health_checks=[],
        analysis_timestamp=analysis_timestamp,
        metadata={},
        file_layout={},
        partition_analysis={},
        history_analysis={},
        optimize_info={},
        growth_analysis={},
        file_skew={},
        sql_commands=[]
    )
    
    try:
        # Step 1: Collect metadata
        logger.info(f"[1/7] Collecting metadata for {full_name}")
        metadata = collect_table_metadata(catalog, schema, table)
        result.metadata = metadata
        
        if not metadata.get("is_valid"):
            result.error = metadata.get("error", "Invalid table or not a Delta table")
            logger.warning(f"Table {full_name} is not valid: {result.error}")
            return result
        
        result.table_size_gb = metadata.get("table_size_gb", 0)
        result.num_files = metadata.get("num_files", 0)
        result.partition_columns = metadata.get("partition_columns", [])
        
        # Step 2: Analyze file layout
        logger.info(f"[2/7] Analyzing file layout for {full_name}")
        table_path = metadata.get("location")
        if table_path:
            file_layout = analyze_file_layout(table_path, config)
            result.file_layout = file_layout
            result.avg_file_size_mb = file_layout.get("avg_file_size_mb", 0)
            
            # Detect file skew
            if file_layout.get("file_sizes"):
                file_skew = detect_file_skew(file_layout["file_sizes"], config)
                result.file_skew = file_skew
                result.skew_ratio = file_skew.get("file_skew_ratio", 0)
        
        # Step 3: Analyze partitions
        logger.info(f"[3/7] Analyzing partitions for {full_name}")
        partition_analysis = analyze_partitions(
            catalog, schema, table, 
            result.partition_columns, config
        )
        result.partition_analysis = partition_analysis
        result.partition_count = partition_analysis.get("partition_count", 0)
        
        # Step 4: Analyze Delta history
        logger.info(f"[4/7] Analyzing Delta history for {full_name}")
        history_analysis = analyze_delta_history(catalog, schema, table, config)
        result.history_analysis = history_analysis
        result.last_optimize = history_analysis.get("last_optimize_timestamp")
        result.last_vacuum = history_analysis.get("last_vacuum_timestamp")
        
        # Step 5: Detect OPTIMIZE operations
        logger.info(f"[5/7] Detecting OPTIMIZE operations for {full_name}")
        optimize_info = detect_optimize_operations(history_analysis.get("history_df"))
        result.optimize_info = optimize_info
        result.zorder_columns = optimize_info.get("zorder_columns", [])
        
        # Step 6: Analyze growth
        logger.info(f"[6/7] Analyzing growth for {full_name}")
        growth_analysis = analyze_table_growth(
            history_analysis.get("history_df"),
            metadata.get("table_size_bytes", 0)
        )
        result.growth_analysis = growth_analysis
        
        # Step 7: Run health checks and generate recommendations
        logger.info(f"[7/7] Running health checks for {full_name}")
        health_checks = run_health_checks(
            metadata, result.file_layout, partition_analysis,
            history_analysis, optimize_info, config
        )
        result.health_checks = health_checks
        
        # Generate recommendations
        recommendations = generate_recommendations(
            full_name, metadata, result.file_layout, partition_analysis,
            result.file_skew, history_analysis, optimize_info, health_checks, config
        )
        result.recommendations_detail = recommendations
        result.recommendations = [r.title for r in recommendations]
        
        # Collect SQL commands
        result.sql_commands = [r.sql_command for r in recommendations if r.sql_command]
        
        logger.info(f"Analysis complete for {full_name}: {len(recommendations)} recommendations")
        
    except Exception as e:
        result.error = str(e)
        logger.error(f"Error analyzing {full_name}: {e}")
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Report Output

# COMMAND ----------

# DBTITLE 1,Report Generation Functions
def create_report_dataframe(results: List[TableAnalysisResult]) -> DataFrame:
    """
    Create a Spark DataFrame from analysis results.
    
    Args:
        results: List of TableAnalysisResult objects
        
    Returns:
        Spark DataFrame with analysis results
    """
    # Define schema
    schema = StructType([
        StructField("catalog", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("table_size_gb", DoubleType(), True),
        StructField("num_files", LongType(), True),
        StructField("avg_file_size_mb", DoubleType(), True),
        StructField("partition_count", IntegerType(), True),
        StructField("skew_ratio", DoubleType(), True),
        StructField("last_optimize", StringType(), True),
        StructField("last_vacuum", StringType(), True),
        StructField("zorder_columns", StringType(), True),
        StructField("recommendations", StringType(), True),
        StructField("sql_commands", StringType(), True),
        StructField("health_status", StringType(), True),
        StructField("analysis_timestamp", StringType(), True),
        StructField("error", StringType(), True)
    ])
    
    # Convert results to rows
    rows = []
    for r in results:
        # Determine health status
        health_status = "healthy"
        if r.health_checks:
            if any(c.status == "fail" for c in r.health_checks):
                health_status = "critical"
            elif any(c.status == "warn" for c in r.health_checks):
                health_status = "warning"
        
        if r.error:
            health_status = "error"
        
        rows.append((
            r.catalog,
            r.schema,
            r.table,
            float(r.table_size_gb),
            int(r.num_files),
            float(r.avg_file_size_mb),
            int(r.partition_count),
            float(r.skew_ratio),
            r.last_optimize,
            r.last_vacuum,
            json.dumps(r.zorder_columns) if r.zorder_columns else "[]",
            json.dumps(r.recommendations) if r.recommendations else "[]",
            json.dumps(r.sql_commands) if r.sql_commands else "[]",
            health_status,
            r.analysis_timestamp,
            r.error
        ))
    
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
    print("=" * 80 + "\n")
    
    for r in results:
        print(f"\n{'─' * 60}")
        print(f"📊 TABLE: {r.full_name}")
        print(f"{'─' * 60}")
        
        if r.error:
            print(f"❌ Error: {r.error}")
            continue
        
        # Basic metrics
        print(f"\n📁 STORAGE METRICS:")
        print(f"   • Table Size:         {r.table_size_gb:.2f} GB")
        print(f"   • Number of Files:    {r.num_files:,}")
        print(f"   • Avg File Size:      {r.avg_file_size_mb:.2f} MB")
        print(f"   • Partitions:         {r.partition_count:,}")
        if r.partition_columns:
            print(f"   • Partition Columns:  {', '.join(r.partition_columns)}")
        
        # Skew info
        if r.skew_ratio > 1:
            print(f"\n⚖️  SKEW ANALYSIS:")
            print(f"   • File Skew Ratio:    {r.skew_ratio:.1f}x")
        
        # Maintenance info
        print(f"\n🔧 MAINTENANCE STATUS:")
        print(f"   • Last OPTIMIZE:      {r.last_optimize or 'Never'}")
        print(f"   • Last VACUUM:        {r.last_vacuum or 'Never'}")
        if r.zorder_columns:
            print(f"   • ZORDER Columns:     {', '.join(r.zorder_columns)}")
        
        # Health checks
        if r.health_checks:
            failed = [c for c in r.health_checks if c.status == "fail"]
            warnings = [c for c in r.health_checks if c.status == "warn"]
            
            print(f"\n🏥 HEALTH STATUS:")
            if failed:
                print(f"   ❌ Critical Issues: {len(failed)}")
                for c in failed:
                    print(f"      • {c.message}")
            if warnings:
                print(f"   ⚠️  Warnings: {len(warnings)}")
                for c in warnings:
                    print(f"      • {c.message}")
            if not failed and not warnings:
                print(f"   ✅ All checks passed")
        
        # Recommendations
        if r.recommendations_detail:
            print(f"\n💡 RECOMMENDATIONS:")
            for i, rec in enumerate(r.recommendations_detail, 1):
                priority_icon = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}.get(rec.priority, "⚪")
                print(f"   {i}. [{priority_icon} {rec.priority.upper()}] {rec.title}")
                print(f"      {rec.description[:100]}...")
                if rec.sql_command:
                    print(f"      SQL: {rec.sql_command}")
        
        # SQL Commands summary
        if r.sql_commands:
            print(f"\n📝 OPTIMIZATION SQL COMMANDS:")
            for cmd in r.sql_commands:
                print(f"   {cmd}")
    
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

# DBTITLE 1,Results DataFrame
# Create and display results as a DataFrame
if results:
    report_df = create_report_dataframe(results)
    display(report_df)
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
                health_status,
                recommendations,
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

# Delta Table Layout Analyzer

A production-grade Databricks notebook for analyzing Delta table storage layout and history to generate optimization recommendations.

## Overview

This tool helps Databricks platform engineers and data engineers:

- **Analyze Delta table file layout** - Detect small files, file size skew, and storage inefficiencies
- **Inspect partition distribution** - Identify partition skew and excess partitions
- **Review Delta history** - Track OPTIMIZE, VACUUM, MERGE operations
- **Generate recommendations** - Get actionable optimization suggestions with SQL commands
- **Monitor table health** - Run comprehensive health checks on your Delta tables

## Features

| Feature | Description |
|---------|-------------|
| **Unity Catalog Support** | Works with managed and external tables |
| **Metadata-Only Analysis** | No full table scans - uses Delta log metadata |
| **Scalable** | Handles thousands of tables efficiently |
| **Configurable Thresholds** | Customize all detection thresholds |
| **SQL Command Generation** | Auto-generates OPTIMIZE and VACUUM commands |
| **Result Persistence** | Store results in Delta for trend analysis |

## Quick Start

### 1. Import the Notebook

Import `delta_layout_analyzer.py` into your Databricks workspace.

### 2. Configure Target Table

```python
config = AnalyzerConfig(
    catalog="myn_monitor_demo",
    schema="observability", 
    table="table_name",
    scan_mode="single_table"  # or "schema" or "catalog"
)
```

### 3. Run Analysis

```python
results = run_analysis(config)
```

## Scan Modes

| Mode | Description |
|------|-------------|
| `single_table` | Analyze a specific table |
| `schema` | Analyze all tables in a schema |
| `catalog` | Analyze all tables in a catalog |

## Configuration Options

```python
@dataclass
class AnalyzerConfig:
    # Target table configuration
    catalog: str = "myn_monitor_demo"
    schema: str = "observability"
    table: str = "table_name"
    scan_mode: str = "single_table"
    
    # File size thresholds (MB)
    small_file_severe_mb: int = 32
    small_file_acceptable_mb: int = 128
    optimal_file_size_min_mb: int = 512
    optimal_file_size_max_mb: int = 1024
    
    # Skew thresholds
    file_skew_threshold: float = 10.0
    partition_skew_threshold: float = 10.0
    
    # Health check thresholds
    max_partition_count: int = 5000
    large_table_threshold_gb: int = 1000
    zorder_recommend_threshold_gb: int = 500
    optimize_stale_days: int = 30
    vacuum_stale_days: int = 30
    
    # Output configuration
    persist_results: bool = True
```

## Analysis Functions

### Core Functions

| Function | Purpose |
|----------|---------|
| `collect_table_metadata()` | Extract table size, files, partitions using DESCRIBE DETAIL |
| `analyze_file_layout()` | Analyze file sizes from Delta log metadata |
| `analyze_partitions()` | Compute partition distribution and skew |
| `detect_file_skew()` | Calculate file size skew ratio |
| `analyze_delta_history()` | Extract operation history from DESCRIBE HISTORY |
| `detect_optimize_operations()` | Identify OPTIMIZE runs and ZORDER columns |
| `analyze_table_growth()` | Compute daily write volume and growth rate |
| `run_health_checks()` | Execute comprehensive health checks |
| `generate_recommendations()` | Produce actionable optimization recommendations |

### Discovery Functions

| Function | Purpose |
|----------|---------|
| `discover_tables()` | Find tables based on scan mode |
| `list_schema_tables()` | List all tables in a schema |
| `list_catalog_tables()` | List all tables in a catalog |
| `is_delta_table()` | Check if table is Delta format |

## Output Report

The analyzer produces a structured report with:

```json
{
  "table": "catalog.schema.table",
  "table_size_gb": 1200,
  "num_files": 450000,
  "avg_file_size_mb": 8,
  "partition_count": 365,
  "file_skew_ratio": 15,
  "last_optimize": "2024-01-01",
  "zorder_columns": [],
  "recommendations": [
    "Run OPTIMIZE",
    "Consider ZORDER BY customer_id",
    "Review partition strategy"
  ],
  "sql_commands": [
    "OPTIMIZE catalog.schema.table;",
    "VACUUM catalog.schema.table RETAIN 168 HOURS;"
  ]
}
```

## Health Checks

| Check | Condition | Severity |
|-------|-----------|----------|
| Small Files | `avg_file_size < 32MB` | Critical |
| Excess Partitions | `partition_count > 5000` | Critical |
| Large Table | `table_size > 1TB` | Warning |
| No Recent OPTIMIZE | `last_optimize > 30 days` | Warning |
| No Recent VACUUM | `last_vacuum > 30 days` | Warning |
| File Skew | `skew_ratio > 10` | Warning |
| Partition Skew | `skew_ratio > 10` | Warning |
| No ZORDER | `table > 500GB AND no_zorder` | Warning |

## Recommendations Generated

The recommendation engine evaluates:

1. **Small Files** → Recommend `OPTIMIZE`
2. **File Skew** → Recommend repartitioning before writes
3. **Large Tables without ZORDER** → Recommend `OPTIMIZE ZORDER BY`
4. **Excess Partitions** → Recommend partition strategy review
5. **Stale OPTIMIZE** → Recommend scheduling optimize jobs
6. **Stale VACUUM** → Recommend running VACUUM

## Persisted Results

Results are stored in a Delta table for trend analysis:

```sql
SELECT * FROM myn_monitor_demo.observability.delta_layout_analysis
ORDER BY analysis_timestamp DESC;
```

### Schema

| Column | Type | Description |
|--------|------|-------------|
| catalog | string | Catalog name |
| schema | string | Schema name |
| table | string | Table name |
| table_size_gb | double | Table size in GB |
| num_files | long | Number of files |
| avg_file_size_mb | double | Average file size in MB |
| partition_count | int | Number of partitions |
| skew_ratio | double | File size skew ratio |
| last_optimize | string | Last OPTIMIZE timestamp |
| last_vacuum | string | Last VACUUM timestamp |
| zorder_columns | string | JSON array of ZORDER columns |
| recommendations | string | JSON array of recommendations |
| sql_commands | string | JSON array of SQL commands |
| health_status | string | healthy/warning/critical/error |
| analysis_timestamp | string | Analysis timestamp |

## Performance Considerations

- **No Full Table Scans**: Uses DESCRIBE DETAIL and Delta log metadata
- **Efficient History Queries**: Limits history records (configurable)
- **Parallel Processing**: Supports parallel table analysis
- **Sampling**: Samples large file lists for statistics

## Example Usage

### Single Table Analysis

```python
config = AnalyzerConfig(
    catalog="my_catalog",
    schema="my_schema",
    table="my_table",
    scan_mode="single_table"
)
results = run_analysis(config)
```

### Schema-Wide Scan

```python
config = AnalyzerConfig(
    catalog="my_catalog",
    schema="my_schema",
    scan_mode="schema",
    persist_results=True
)
results = run_analysis(config)
```

### Custom Thresholds

```python
config = AnalyzerConfig(
    catalog="my_catalog",
    schema="my_schema",
    table="my_table",
    small_file_severe_mb=64,
    file_skew_threshold=5.0,
    optimize_stale_days=14
)
results = run_analysis(config)
```

## Requirements

- Databricks Runtime 11.3+
- Unity Catalog enabled
- Access to target tables
- Delta Lake tables

## License

Internal use only.

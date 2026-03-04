# Delta Table Layout Analyzer

A production-grade Databricks notebook for analyzing Delta table storage layout and history to generate optimization recommendations.

## Overview

This tool helps Databricks platform engineers and data engineers:

- **Analyze Delta table file layout** - Detect small files with percentage breakdown
- **Detect file size skew** - Identify skew severity levels (low/medium/high)
- **Inspect partition distribution** - Identify partition skew and excess partitions
- **Check Liquid Clustering** - Detect if tables use Liquid Clustering
- **Review Delta history** - Track OPTIMIZE, VACUUM, MERGE operations
- **Generate recommendations** - Get actionable optimization suggestions with SQL commands
- **Monitor table health** - Comprehensive health status based on all issues

## Features

| Feature | Description |
|---------|-------------|
| **Unity Catalog Support** | Works with managed and external tables |
| **Accurate File Size Detection** | Uses `dbutils.fs.ls` for precise individual file sizes |
| **Scalable** | Handles thousands of tables efficiently |
| **Clear Issue Tracking** | Each issue has category, severity, details, and recommendation |
| **Liquid Clustering Detection** | Checks DDL for CLUSTER BY clause |
| **SQL Command Generation** | Auto-generates OPTIMIZE, VACUUM, and ALTER TABLE commands |
| **Health Status** | Overall status: healthy/needs_attention/warning/critical |

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

---

## Health Check Logic

The analyzer performs the following checks with specific logic for each:

### 1. Small Files Detection

**Purpose:** Detect tables with too many small files that impact query performance.

**How File Sizes are Collected:**

The analyzer uses `dbutils.fs.ls` to get accurate individual file sizes:

```python
# Get table location from DESCRIBE DETAIL
detail_df = spark.sql("DESCRIBE DETAIL catalog.schema.table")
table_location = detail_df.first().location

# List all files using dbutils.fs.ls
files = dbutils.fs.ls(table_location)

# Each file returns FileInfo with size:
# FileInfo(
#     path='s3://bucket/table/part-00000.parquet',
#     name='part-00000.parquet',
#     size=46804904,  # <-- Actual file size in bytes
#     modificationTime=1772614999000
# )
```

This method provides:
- **Accurate individual file sizes** (not estimates)
- **File modification times** for age analysis
- **Full file paths** for debugging
- **Recursive scanning** for partitioned tables

**Fallback:** If `dbutils.fs.ls` is not available, falls back to parsing Delta transaction log.

**Logic:**
```
IF avg_file_size < 32MB OR severe_small_files_pct > 50%:
    severity = CRITICAL
    
ELIF avg_file_size < 128MB OR small_files_pct > 20%:
    severity = MEDIUM
    
ELSE:
    severity = NONE (healthy)
```

**Metrics Collected:**
- Average file size (MB)
- Median file size (MB)
- Min/Max file sizes (MB)
- Standard deviation (MB)
- Percentage of small files (<128MB)
- Percentage of severe small files (<32MB)
- Total file count

**Thresholds:**
| Threshold | Default Value | Description |
|-----------|---------------|-------------|
| `small_file_severe_mb` | 32 MB | Files below this are severely small |
| `small_file_acceptable_mb` | 128 MB | Files below this are considered small |
| `small_files_pct_warning` | 20% | Warning if more than this % are small |
| `small_files_pct_critical` | 50% | Critical if more than this % are small |

---

### 2. File Skew Detection

**Purpose:** Detect uneven file size distribution that causes query performance issues.

**How Skew is Calculated:**

Using the individual file sizes from `dbutils.fs.ls`:

```python
# Example file sizes from dbutils.fs.ls
file_sizes = [46804904, 44643673, 52425955, 50607679, 54610255, 51097757]  # bytes

# Calculate statistics
median_size = statistics.median(file_sizes)  # 50852718
max_size = max(file_sizes)                    # 54610255
min_size = min(file_sizes)                    # 44643673

# Skew ratio = max / median
skew_ratio = max_size / median_size  # 1.07x (no significant skew)
```

**Logic:**
```
skew_ratio = max_file_size / median_file_size

IF skew_ratio <= 3.0:
    severity = NONE (no significant skew)
    
ELIF skew_ratio <= 5.0:
    severity = LOW (minor skew, monitor)
    
ELIF skew_ratio <= 10.0:
    severity = MEDIUM (moderate skew, consider action)
    
ELSE:
    severity = HIGH (significant skew, action required)
```

**Metrics Collected:**
- File skew ratio (max/median)
- Coefficient of variation
- Min/max/median file sizes

**Thresholds:**
| Threshold | Default Value | Description |
|-----------|---------------|-------------|
| `skew_low_threshold` | 3.0 | Below this = no skew |
| `skew_medium_threshold` | 5.0 | Above this = low skew |
| `skew_high_threshold` | 10.0 | Above this = medium/high skew |

---

### 3. OPTIMIZE Status Check

**Purpose:** Ensure tables are regularly optimized for query performance.

**Logic:**
```
IF last_optimize_timestamp IS NULL:
    severity = HIGH
    status = "Never run"
    
ELIF days_since_last_optimize > 30:
    severity = MEDIUM
    status = "{days} days ago"
    
ELSE:
    severity = NONE (healthy)
    status = "{days} days ago"
```

**Metrics Collected:**
- Last OPTIMIZE timestamp
- Days since last OPTIMIZE
- Total OPTIMIZE count in history

**Threshold:**
| Threshold | Default Value | Description |
|-----------|---------------|-------------|
| `optimize_stale_days` | 30 | OPTIMIZE is stale after this many days |

---

### 4. VACUUM Status Check

**Purpose:** Ensure old files are cleaned up to reclaim storage.

**Logic:**
```
IF last_vacuum_timestamp IS NULL:
    severity = MEDIUM
    status = "Never run"
    
ELIF days_since_last_vacuum > 30:
    severity = LOW
    status = "{days} days ago"
    
ELSE:
    severity = NONE (healthy)
    status = "{days} days ago"
```

**Metrics Collected:**
- Last VACUUM timestamp
- Days since last VACUUM
- Total VACUUM count in history

**Threshold:**
| Threshold | Default Value | Description |
|-----------|---------------|-------------|
| `vacuum_stale_days` | 30 | VACUUM is stale after this many days |

---

### 5. Large Table Partitioning Check

**Purpose:** Ensure large tables (>1TB) are partitioned for manageability.

**Logic:**
```
IF table_size_gb >= 1000 AND is_partitioned = FALSE:
    severity = HIGH
    recommendation = "Consider partitioning by date or high-cardinality column"
```

**Threshold:**
| Threshold | Default Value | Description |
|-----------|---------------|-------------|
| `large_table_threshold_gb` | 1000 (1TB) | Tables above this should be partitioned |

---

### 6. Liquid Clustering Check

**Purpose:** For smaller tables (<1TB), recommend Liquid Clustering for automatic optimization.

**Logic:**
```
# Check DDL using SHOW CREATE TABLE
ddl = SHOW CREATE TABLE catalog.schema.table

# Look for CLUSTER BY clause in DDL
IF "CLUSTER BY" in ddl:
    is_liquid_clustered = TRUE
    extract clustering_columns from CLUSTER BY clause
ELSE:
    is_liquid_clustered = FALSE

# Recommendation logic
IF table_size_gb < 1000 AND table_size_gb > 10 AND is_liquid_clustered = FALSE:
    severity = MEDIUM
    recommendation = "Enable Liquid Clustering with ALTER TABLE ... CLUSTER BY"
```

**How Liquid Clustering is Detected:**
1. Execute `SHOW CREATE TABLE catalog.schema.table`
2. Parse the DDL output for `CLUSTER BY (column1, column2)` pattern
3. Extract clustering columns if present

**Example DDL with Liquid Clustering:**
```sql
CREATE TABLE catalog.schema.table (
    id BIGINT,
    name STRING,
    created_at TIMESTAMP
)
USING delta
CLUSTER BY (id, created_at)
```

---

### 7. ZORDER Check (Large Tables)

**Purpose:** Ensure large tables use ZORDER for query optimization.

**Logic:**
```
IF table_size_gb > 500 AND no_zorder_columns_in_history AND NOT is_liquid_clustered:
    severity = MEDIUM
    recommendation = "Run OPTIMIZE with ZORDER BY on filter columns"
```

**How ZORDER is Detected:**
1. Query `DESCRIBE HISTORY` for OPTIMIZE operations
2. Parse `operationParameters` for `zOrderBy` field
3. Extract ZORDER columns from history

**Threshold:**
| Threshold | Default Value | Description |
|-----------|---------------|-------------|
| `zorder_recommend_threshold_gb` | 500 | Tables above this should use ZORDER |

---

### 8. Excess Partitions Check

**Purpose:** Detect tables with too many partitions causing metadata overhead.

**Logic:**
```
IF partition_count > 5000:
    severity = CRITICAL
    
ELIF partition_count > 4000:  # 80% of limit
    severity = MEDIUM
    
ELSE:
    severity = NONE
```

**Threshold:**
| Threshold | Default Value | Description |
|-----------|---------------|-------------|
| `max_partition_count` | 5000 | Maximum recommended partitions |

---

### 9. Partition Skew Check

**Purpose:** Detect uneven partition sizes that cause query hotspots.

**Logic:**
```
partition_skew_ratio = largest_partition_size / median_partition_size

# Same severity logic as file skew:
IF partition_skew_ratio <= 3.0: NONE
ELIF partition_skew_ratio <= 5.0: LOW
ELIF partition_skew_ratio <= 10.0: MEDIUM
ELSE: HIGH
```

---

## Final Health Status Calculation

The overall health status is computed based on detected issues:

```python
def compute_health_status(issues):
    severities = [issue.severity for issue in issues]
    
    IF "critical" in severities:
        return "CRITICAL"
        
    ELIF "high" in severities:
        return "WARNING"
        
    ELIF "medium" in severities:
        return "NEEDS_ATTENTION"
        
    ELSE:
        return "HEALTHY"
```

| Health Status | Description | When Assigned |
|---------------|-------------|---------------|
| `HEALTHY` | No issues | Only low/no severity issues |
| `NEEDS_ATTENTION` | Minor issues | At least one medium severity issue |
| `WARNING` | Significant issues | At least one high severity issue |
| `CRITICAL` | Urgent issues | At least one critical severity issue |

---

## Output Columns

The analyzer produces a DataFrame with clear columns:

### Storage Metrics
| Column | Description |
|--------|-------------|
| `table_size_gb` | Table size in GB |
| `num_files` | Total number of files |
| `avg_file_size_mb` | Average file size in MB |
| `small_files_pct` | Percentage of files <128MB |
| `severe_small_files_pct` | Percentage of files <32MB |

### Skew Metrics
| Column | Description |
|--------|-------------|
| `file_skew_ratio` | max_file_size / median_file_size |
| `file_skew_severity` | none/low/medium/high |
| `partition_skew_ratio` | largest_partition / median_partition |

### Partitioning
| Column | Description |
|--------|-------------|
| `is_partitioned` | Boolean - is table partitioned |
| `partition_count` | Number of partitions |
| `partition_columns` | List of partition columns |

### Clustering
| Column | Description |
|--------|-------------|
| `is_liquid_clustered` | Boolean - uses Liquid Clustering |
| `clustering_columns` | Columns in CLUSTER BY clause |
| `zorder_columns` | ZORDER columns from history |

### Maintenance Status
| Column | Description |
|--------|-------------|
| `last_optimize` | Timestamp of last OPTIMIZE |
| `last_optimize_days_ago` | Days since last OPTIMIZE |
| `optimize_status` | Human-readable status |
| `last_vacuum` | Timestamp of last VACUUM |
| `last_vacuum_days_ago` | Days since last VACUUM |
| `vacuum_status` | Human-readable status |

### Health Status
| Column | Description |
|--------|-------------|
| `health_status` | healthy/needs_attention/warning/critical |
| `issue_count` | Total number of issues |
| `critical_issues` | Count of critical issues |
| `high_issues` | Count of high severity issues |
| `medium_issues` | Count of medium severity issues |
| `low_issues` | Count of low severity issues |

---

## Issue Categories

Each detected issue belongs to a category:

| Category | Description |
|----------|-------------|
| `small_files` | Small file compaction needed |
| `file_skew` | Uneven file size distribution |
| `partition_skew` | Uneven partition sizes |
| `stale_optimize` | OPTIMIZE not run recently |
| `stale_vacuum` | VACUUM not run recently |
| `excess_partitions` | Too many partitions |
| `no_partitioning` | Large table not partitioned |
| `no_liquid_clustering` | Table should use Liquid Clustering |
| `no_zorder` | Large table without ZORDER |

---

## Configuration Options

```python
@dataclass
class AnalyzerConfig:
    # Target table configuration
    catalog: str = "myn_monitor_demo"
    schema: str = "observability"
    table: str = "table_name"
    scan_mode: str = "single_table"  # "single_table", "schema", "catalog"
    
    # File size thresholds (MB)
    small_file_severe_mb: int = 32
    small_file_acceptable_mb: int = 128
    optimal_file_size_min_mb: int = 512
    optimal_file_size_max_mb: int = 1024
    
    # Skew thresholds
    file_skew_threshold: float = 10.0
    partition_skew_threshold: float = 10.0
    skew_low_threshold: float = 3.0
    skew_medium_threshold: float = 5.0
    skew_high_threshold: float = 10.0
    
    # Small files percentage thresholds
    small_files_pct_warning: float = 20.0
    small_files_pct_critical: float = 50.0
    
    # Health check thresholds
    max_partition_count: int = 5000
    large_table_threshold_gb: int = 1000  # 1TB
    zorder_recommend_threshold_gb: int = 500
    optimize_stale_days: int = 30
    vacuum_stale_days: int = 30
    
    # Output configuration
    persist_results: bool = True
```

---

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
    optimize_stale_days=14,
    large_table_threshold_gb=500
)
results = run_analysis(config)
```

---

## Sample Output

### Summary Report
```
✅ TABLE: catalog.schema.healthy_table
   Health Status: HEALTHY
   
🔴 TABLE: catalog.schema.problem_table
   Health Status: CRITICAL
   
   📁 STORAGE METRICS:
      • Table Size:           150.00 GB
      • Number of Files:      45,000
      • Avg File Size:        3.33 MB
      • Small Files (<128MB): 95.2%
      • Severe Small (<32MB): 89.1%
   
   ⚠️  ISSUES DETECTED (3):
      • Critical: 1, High: 1, Medium: 1
   
   🔴 [CRITICAL] Critical Small Files Issue
      Category: small_files
      Current:  3.33 MB avg, 89.1% severe small files
      Expected: >32 MB avg, <50% small files
      Action:   Run OPTIMIZE immediately
      SQL:      OPTIMIZE catalog.schema.problem_table;
```

### Issues DataFrame
| catalog | schema | table | issue_id | category | severity | title | details |
|---------|--------|-------|----------|----------|----------|-------|---------|
| my_cat | my_sch | tbl1 | SMALL_FILES_CRITICAL | small_files | critical | Critical Small Files | Avg 3.33 MB... |
| my_cat | my_sch | tbl1 | OPTIMIZE_NEVER_RUN | stale_optimize | high | OPTIMIZE Never Run | Table never optimized |

---

## Requirements

- Databricks Runtime 11.3+
- Unity Catalog enabled
- Access to target tables
- Delta Lake tables

## Performance

- **No Full Table Scans**: Never reads actual table data
- **Accurate File Sizes**: Uses `dbutils.fs.ls` for precise individual file metrics
- **Efficient History Queries**: Uses `DESCRIBE HISTORY` with configurable limits
- **Metadata Only**: Uses `DESCRIBE DETAIL` for table-level metrics
- **Sampling**: Samples up to 50,000 files for very large tables
- **Recursive Scanning**: Handles partitioned tables by recursively listing directories

### Data Collection Methods

| Data | Method | Notes |
|------|--------|-------|
| Table size, file count | `DESCRIBE DETAIL` | Fast metadata query |
| Individual file sizes | `dbutils.fs.ls` | Accurate, includes modification times |
| Partition columns | `DESCRIBE DETAIL` | From partitionColumns field |
| Liquid Clustering | `SHOW CREATE TABLE` | Parses DDL for CLUSTER BY |
| OPTIMIZE/VACUUM history | `DESCRIBE HISTORY` | Limited to recent records |
| ZORDER columns | `DESCRIBE HISTORY` | Parsed from operationParameters |

---

## License

Internal use only.

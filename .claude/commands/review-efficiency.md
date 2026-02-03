Review the materialization strategy and query efficiency of this dbt project for BigQuery at scale.

## Rules to Check

### Materialization Strategy
- Each incremental model must have a clear reason for being incremental (expensive computation, large data volume, or multiple downstream consumers)
- Views should only be used when the underlying query is cheap to re-execute (simple joins to small lookup tables, no heavy aggregations)
- Avoid materializing intermediate models that are only read by one downstream model (use views instead)
- Models read by multiple downstream consumers should be materialized to avoid redundant computation

### Incremental Filter Placement
- The `{% if is_incremental() %}` filter must live INSIDE the model that owns the expensive operation — never rely on filtering a view's output, because BigQuery evaluates the full view before applying outer filters
- Window functions (`row_number`, `rank`, `lag`, etc.) block predicate pushdown — BigQuery must evaluate the full window before filtering, so incremental filters must be applied BEFORE the window function in the same query
- Verify that incremental filters target the correct column (the column that changes over time, typically `updated_at`, not `created_at`)

### Partition Pruning Through Views
- BigQuery CAN prune partitions through views when the filter references the partition column directly (including simple renames/aliases)
- BigQuery CANNOT prune partitions through views that contain aggregations (`GROUP BY`), `UNION ALL`, or complex transformations on the partition column
- Check each view in the DAG: if it sits between a partitioned table and a downstream consumer, verify the partition column is passed through without transformation
- Flag any aggregation views that block partition pruning from reaching upstream partitioned tables

### Partition & Clustering Configuration
- Every incremental table should have a `partition_by` on a timestamp or date column relevant to the incremental filter
- Partition column should be immutable (e.g., `created_at`, not `status`) so rows don't move between partitions on updates
- `cluster_by` should include the `unique_key` and any frequently joined columns
- Marts with merge strategy on non-partition keys (e.g., merge on `user_id` with partition on `created_at`) get limited pruning benefit — verify this trade-off is intentional
- Flag incremental tables with NO partition — every merge touches the entire table

### Redundant Computation
- Flag models where the same view chain is evaluated multiple times in a single downstream query (e.g., a mart that references `int_orders_enriched` in both a CTE and a subquery, causing BigQuery to evaluate the view twice)
- Flag `GROUP BY` views that full-scan an upstream table — consider whether the aggregation could be pushed into an incremental model
- Check for `select *` that pulls unnecessary columns through expensive view chains

### BigQuery-Specific Patterns
- `MERGE` statements on unpartitioned tables cause full table scans of the target — flag these
- `CREATE TABLE AS SELECT` (CTAS) followed by `MERGE` is the standard dbt incremental pattern — verify the CTAS doesn't scan more data than necessary
- Check for division operations that could cause division-by-zero (e.g., `amount / usd_rate` where `usd_rate` could be null or zero)
- Verify `on_schema_change` is set appropriately for incremental models

### Cost Analysis
- Query `INFORMATION_SCHEMA.JOBS` to measure actual bytes processed and slot time for recent builds
- Compare incremental vs full-refresh costs
- Identify the most expensive model in the DAG by slot time and bytes processed
- Flag any model where incremental run costs are close to full-refresh costs (indicates the incremental strategy isn't providing value)

## Instructions
1. Read all SQL models and their YAML configs
2. Map the full DAG with materialization type for each node
3. For each incremental model, verify the incremental filter placement is optimal (before expensive operations, not after)
4. Trace partition pruning paths through the view chain — identify where pruning is blocked
5. Check for redundant view evaluations in mart queries
6. Run a test build and query `INFORMATION_SCHEMA.JOBS` to capture actual BigQuery metrics:
   ```sql
   select
       destination_table.table_id as target_table,
       statement_type,
       round(total_bytes_processed / pow(1024, 2), 2) as mb_processed,
       round(total_bytes_billed / pow(1024, 2), 2) as mb_billed,
       round(total_slot_ms / 1000.0, 2) as slot_seconds,
       timestamp_diff(end_time, start_time, millisecond) as wall_time_ms,
       cache_hit
   from `region-us`.INFORMATION_SCHEMA.JOBS
   where creation_time > timestamp_sub(current_timestamp(), interval 30 minute)
     and job_type = 'QUERY'
     and state = 'DONE'
     and error_result is null
     and statement_type in ('CREATE_TABLE_AS_SELECT', 'MERGE')
   order by total_slot_ms desc
   ```
7. Report findings with severity (error/warning/info), file path, line number, rule violated, and suggested fix
8. Provide a materialization summary table and a prioritized list of optimizations

Review staging models in this dbt project against official dbt best practices.

## Rules to Check

### Structure & Organization
- Staging models MUST have a 1-to-1 relationship with source tables
- Each staging model is the ONLY place the `source()` macro is used for that table
- Subdirectories should be based on source system (e.g., `staging/stripe/`), NOT by loader or business grouping
- File naming: `stg_[source]__[entity]s.sql` (double underscore, pluralized)

### Allowed Transformations ONLY
- Renaming columns for clarity and consistency
- Type casting (string to integer, timestamp conversion)
- Basic computations (e.g., cents to dollars conversion)
- Categorizing with CASE WHEN to group values into buckets or booleans

### Required Patterns
- Column selection must be explicit — NO `select *` from source
- Columns should be organized by type: IDs, strings, numerics, booleans, dates, timestamps
- Primary keys should be cast to string type
- Booleans should use `is_` or `has_` prefix
- Timestamps should use `<event>_at` naming (UTC)
- Dates should use `<event>_date` naming
- Price/revenue fields should be in decimal currency (not cents)

### Standard Model Structure
```sql
with source as (
    select
        column1,
        column2
    from {{ source('source_name', 'table_name') }}
)

select
    id as entity_id,
    cast(created as timestamp) as created_at,
    amount / 100.0 as amount,
    case when status = 'active' then true else false end as is_active
from source
```

### Materialization
- Staging models MUST be materialized as views (set in dbt_project.yml)
- Never materialize staging as tables or incremental

### What is FORBIDDEN in Staging
- NO joins — staging models clean individual source-conformed concepts only
- NO aggregations — aggregations change grain and belong downstream
- NO complex business logic — keep it simple, push complexity to intermediate
- NO deduplication logic — handle in intermediate layer
- NO window functions for business logic — belongs downstream
- Exception: Joins are acceptable in `base/` sub-models for unioning or handling delete tables

### Testing
- Do NOT add tests that re-test your own cleanup operations
- DO test for business-focused anomalies: values outside acceptable ranges, unexpected negatives, volume spikes
- Every staging model should have primary key tested for unique + not_null (in a schema.yml)

### DRY Principle
- If a transformation will be needed in every downstream model, do it in staging
- Push transformations upstream as far as possible to eliminate repeated code

## Instructions
1. Read all models in `models/staging/`
2. Check each model against every rule above
3. Flag any joins, aggregations, or complex logic that belongs downstream
4. Verify naming conventions match `stg_[source]__[entity]s` pattern
5. Check that `select *` is not used anywhere
6. Verify materialization is set to view
7. Report findings with specific file paths and line numbers

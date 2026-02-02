Run a comprehensive review of this entire dbt project against all official dbt best practices.

Execute each of the following review commands and compile a unified report:

## Review Steps

### 1. Project Structure
- Verify folder structure matches dbt conventions:
  - `models/staging/[source_system]/` — organized by source
  - `models/intermediate/[business_function]/` — organized by business area
  - `models/marts/[business_function]/` — organized by business area
- YAML files prefixed with underscore (`_sources.yml`, `_models.yml`)
- Check `dbt_project.yml` for correct materialization defaults per layer:
  - staging: view
  - intermediate: ephemeral or view
  - marts: table
- Verify `.sqlfluff` config exists and matches dbt style guide

### 2. Sources Review
- Check all `_sources.yml` files for proper configuration
- Verify freshness is configured inside `config:` block
- Ensure source tests only flag fixable-at-source issues
- Confirm `source()` macro is ONLY used in staging models

### 3. Staging Review
- Verify 1-to-1 relationship with source tables
- Check for forbidden patterns: joins, aggregations, complex logic, deduplication
- Verify naming: `stg_[source]__[entity]s`
- Confirm no `select *`
- Check column naming conventions (is_, has_, _at, _date, _id)
- Verify materialized as views

### 4. Intermediate Review
- Check naming follows `int_[entity]s_[verb]ed` with action verbs
- Verify no `source()` usage — only `ref()`
- Confirm appropriate use cases: structural simplification, re-graining, isolating complexity
- Check materialization is ephemeral or view

### 5. Marts Review
- Verify plain English pluralized entity names
- Check for over-complex joins that should use intermediate models
- Confirm no `source()` usage
- Flag time-based rollups or per-department concept duplication
- Verify materialized as table or incremental

### 6. SQL Style
- Run `uv run sqlfluff lint models/` and report results
- Check all models for style violations manually:
  - Lowercase keywords, functions, types
  - Trailing commas
  - Explicit `as` for aliases
  - Explicit join types (`inner join` not `join`)
  - No `select *`
  - 80 char max line length

### 7. Testing
- Verify every model has primary key tested (unique + not_null)
- Check source tests are only for fixable issues
- Verify staging tests focus on business anomalies, not cleanup re-testing
- Check intermediate has primary key tests on re-grained models
- Check marts have tests on complex calculated fields

### 8. DAG Review
- Run `uv run dbt ls --resource-type model` to see all models
- Check for models with too many direct source references
- Flag any `source()` usage outside staging
- Look for circular or overly complex dependencies

## Instructions
1. Execute ALL review steps above
2. For each finding, include: severity (error/warning/info), file path, line number, rule violated, and suggested fix
3. Group findings by layer (sources, staging, intermediate, marts, general)
4. Provide a summary score and prioritized action items

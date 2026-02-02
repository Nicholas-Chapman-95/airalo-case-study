Review source configurations in this dbt project against official dbt best practices.

## Rules to Check

### Structure
- Sources MUST be defined in `_[source]__sources.yml` files within the corresponding staging subdirectory
- The `source()` macro should ONLY be used inside staging models — never referenced directly by intermediate or mart models
- YAML config files should be prefixed with underscore to sort to the top of directory listings

### Freshness
- Source freshness should be configured using `loaded_at_field` inside `config:` block
- Use `error` severity for sources feeding top-priority/customer-facing pipelines
- Use `warn` severity for less critical sources
- `freshness` should be nested inside `config:` (not top-level) to avoid deprecation warnings

### Testing
- Source tests should ONLY flag issues that are fixable at the source system
- "Fixable" means: you can fix it yourself, or you know the right person and can get it fixed
- If source tests flag issues that are NOT fixable at the source, remove the test and mitigate in the staging layer instead
- Recommended source tests:
  - Source freshness for critical pipelines
  - Duplicate primary keys that can be deleted at source
  - Null records (names, emails) that can be entered at source
  - Primary key uniqueness where duplicates are removable at source
- Do NOT test for issues that are inherent to the source system design (e.g., multiple rows per key due to status changes)

### Anti-Patterns to Flag
- `source()` used outside of staging models
- Missing freshness configuration on critical sources
- Tests on source issues that cannot be fixed at the source
- `loaded_at_field` or `freshness` defined as top-level properties instead of inside `config:`
- Missing `not_null` or `unique` tests on primary keys (where fixable)

## Instructions
1. Read all `_sources.yml` files in the project
2. Check each rule above
3. Search for any `source()` usage outside of `models/staging/`
4. Report findings with specific file paths and line numbers
5. Suggest fixes for any violations found

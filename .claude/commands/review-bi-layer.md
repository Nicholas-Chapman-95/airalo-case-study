Review this dbt project's Lightdash BI layer configuration against best practices.

## Rules to Check

### Metrics Configuration
- Every mart model should have Lightdash metrics defined via `config.meta.metrics`
- All aggregate metrics must specify a `type` (sum, count, count_distinct, average, min, max, median)
- Non-aggregate metrics (type: number) must only reference other metrics using `${metric_name}` syntax, never dimensions
- All currency metrics must have `format: '[$]#,##0.00'`
- All percentage metrics must have `format: '0.0%'`
- All metrics should have a `label` and `description`
- Metrics should be organized into `groups` for sidebar navigation

### Dimension Configuration
- All mart columns should have `config.meta.dimension` configured
- Internal/technical columns should be `hidden: true`:
  - Raw currency codes when a human-readable name column exists
  - Boolean status flags used only for metric calculation (is_completed, is_refunded, is_failed)
  - Raw amounts in original currency when a USD column exists
  - Intermediate timestamps not useful for direct exploration (previous_purchase_at, customer_first_purchase_at)
- All visible dimensions should be assigned to `groups` for sidebar organization
- Timestamps should have descriptive labels (e.g. "Order Date" not "created_at")
- ISO code columns should have labels distinguishing them from name columns (e.g. "Country Code" vs "Country")

### Column Descriptions
- Every column in mart models must have a `description`
- Descriptions should be written for business users, not engineers
- Abbreviations should be spelled out when ambiguous (e.g. "Gigabytes" not "GB" to avoid confusion with "Great Britain")
- Descriptions should explain what the column means, not how it was computed

### Metric Types by Column
- Revenue/amount columns: should have sum, average, and optionally median/min/max metrics
- ID columns: should have count and/or count_distinct metrics
- Boolean columns: should have sum metrics (BigQuery treats true as 1)
- Date/interval columns: should have average metrics where meaningful
- Calculated rates: should be model-level non-aggregate metrics (type: number) using nullif() to prevent division by zero

### Metric Filters
- Revenue metrics should filter to appropriate statuses (e.g. `filters: [{is_completed: true}]`)
- Filtered metrics must only be aggregate type (filters are not supported on non-aggregate metrics)
- Filters should match the business definition described in the metric description

### Model-Level Metrics
- Derived ratios (completion rate, refund rate, revenue per customer) should be model-level non-aggregate metrics
- Must reference only other metrics using `${metric_name}` syntax
- Must use `nullif()` for any division to prevent division by zero
- Should be grouped under a "Rates" group when they represent proportions/percentages

### Drill-Down Configuration
- High-level metrics (total revenue, total orders) should have `show_underlying_values` configured
- Show only the most relevant 4-6 columns for drill-down (ID, amount, status, date, key dimension)
- Avoid exposing all 20+ columns in drill-down views

### Formatting Consistency
- Currency columns and metrics: `'[$]#,##0.00'`
- Percentage metrics: `'0.0%'`
- Decimal averages (days, counts): `'#,##0.0'`
- Integer counts: no format needed (Lightdash defaults are fine)
- Use `compact: thousands` or `compact: millions` for dashboard-level metrics where appropriate

### Structural Requirements
- Mart models should be denormalized (Lightdash works best with wide, flat tables)
- Reference data (country names, category labels) should be joined in the mart, not left to the BI layer
- Marts should include both code columns AND human-readable name columns for categorical data
- The human-readable column should be positioned directly after its code column

### Cross-Model Consistency
- Metrics with the same name across models should have consistent definitions
- Shared dimensions (like country, platform) should use the same labels and groups
- Revenue metrics should all use the same currency format

## Instructions

1. Read all mart models (SQL and YAML) in `models/marts/`
2. For each mart model, check every column has `config.meta` with dimension configuration
3. Verify all metrics have correct types, labels, descriptions, and formatting
4. Check non-aggregate metrics reference only other metrics (not dimensions)
5. Confirm internal/technical columns are hidden appropriately
6. Verify all visible dimensions and metrics are assigned to groups
7. Check currency formatting is applied consistently across all models
8. Verify drill-down (`show_underlying_values`) is configured for key metrics
9. Confirm reference data (country names, etc.) is joined in the mart SQL
10. Check cross-model consistency for shared metrics and dimensions
11. Report findings organized by severity:
    - **Errors**: Will break or produce wrong results in Lightdash
    - **Warnings**: Suboptimal configuration that degrades the BI experience
    - **Suggestions**: Nice-to-have improvements

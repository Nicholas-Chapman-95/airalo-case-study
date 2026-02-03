-- depends_on: {{ ref('int_orders_enriched') }}

-- partition on signup_at benefits BI read queries only, not the
-- merge (which matches on user_id regardless of partition).
-- daily granularity gives tighter pruning for date-filtered
-- dashboards; ~10 years before hitting BQ's 4000 partition limit.

{{
    config(
        materialized='incremental',
        unique_key='user_id',
        incremental_strategy='merge',
        partition_by={
            "field": "signup_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["user_id"],
        on_schema_change='append_new_columns'
    )
}}

with

{% if is_incremental() %}

-- customers who had order changes in this batch.
-- NOTE: currently every user has at least one order, so order
-- changes alone capture all affected customers.  If the source
-- data ever includes users who sign up before placing an order,
-- add a UNION DISTINCT on stg_raw__users (filtered by created_at
-- in the var window) so new signups appear here immediately.
customers_to_recompute as (

    select distinct user_id
    from {{ ref('int_orders_enriched') }}
    where order_updated_at >= timestamp('{{ var("data_interval_start") }}')
      and order_updated_at < timestamp('{{ var("data_interval_end") }}')

),

{% endif %}

customer_orders as (

    select
        user_id,
        total_orders,
        completed_orders,
        refunded_orders,
        failed_orders,
        total_purchases,
        total_revenue_usd,
        total_refunded_usd,
        first_order_at,
        last_order_at,
        first_purchase_at,
        last_purchase_at,
        last_order_updated_at,
        days_to_second_purchase
    from {{ ref('int_orders_aggregated_to_customers') }}

    {% if is_incremental() %}
    where user_id in (
        select user_id from customers_to_recompute
    )
    {% endif %}

)

select
    -- customer grain
    u.user_id,

    -- customer attributes: these are signup-time metadata from
    -- stg_raw__users (one row per user, no change tracking in source).
    -- in this customer-grain table the values are unambiguous, so
    -- they don't carry the 'signup_' prefix used in the orders mart
    u.platform,
    u.acquisition_channel,
    u.ip_country,
    c.country_name as ip_country_name,
    u.created_at as signup_at,

    -- order counts
    coalesce(co.total_orders, 0) as total_orders,
    coalesce(co.completed_orders, 0) as completed_orders,
    coalesce(co.refunded_orders, 0) as refunded_orders,
    coalesce(co.failed_orders, 0) as failed_orders,
    coalesce(co.total_purchases, 0) as total_purchases,

    -- revenue
    coalesce(co.total_revenue_usd, 0) as total_revenue_usd,
    coalesce(co.total_refunded_usd, 0) as total_refunded_usd,

    -- lifecycle dates
    co.first_order_at,
    co.last_order_at,
    co.first_purchase_at,
    co.last_purchase_at,

    -- derived lifecycle
    date_diff(
        cast(co.last_order_at as date),
        cast(co.first_order_at as date),
        day
    ) as customer_lifetime_days,

    -- repeat flag
    coalesce(co.total_purchases, 0) > 1 as is_repeat_purchaser,

    -- cohort
    date_trunc(co.first_purchase_at, month) as acquisition_cohort_month,

    -- standardized repeat windows
    co.days_to_second_purchase,
    coalesce(co.days_to_second_purchase <= 90, false) as has_repeat_within_90d,
    coalesce(co.days_to_second_purchase <= 365, false) as has_repeat_within_365d

from {{ ref('stg_raw__users') }} as u

{% if is_incremental() %}
inner join customers_to_recompute as cr
    on u.user_id = cr.user_id
{% endif %}

left join customer_orders as co
    on u.user_id = co.user_id
left join {{ ref('seed_countries') }} as c
    on u.ip_country = c.country_code

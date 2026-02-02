{{
    config(
        materialized='incremental',
        unique_key='user_id',
        incremental_strategy='merge',
        cluster_by=["user_id"],
        on_schema_change='append_new_columns'
    )
}}

with

{% if is_incremental() %}

-- customers with order changes in this batch
-- OR new signups not yet in the table
customers_to_recompute as (

    select distinct user_id
    from {{ ref('orders') }}
    where order_updated_at >= timestamp('{{ var("data_interval_start") }}')
      and order_updated_at < timestamp('{{ var("data_interval_end") }}')

    union distinct

    select user_id
    from {{ ref('stg_raw__users') }}
    where created_at >= timestamp('{{ var("data_interval_start") }}')
      and created_at < timestamp('{{ var("data_interval_end") }}')

),

{% endif %}

customer_orders as (

    select
        user_id,

        -- order counts
        count(*) as total_orders,
        countif(order_is_completed) as completed_orders,
        countif(order_is_refunded) as refunded_orders,
        countif(order_is_failed) as failed_orders,
        countif(not order_is_failed) as total_purchases,

        -- revenue
        sum(case when order_is_completed then order_amount_usd else 0 end)
            as total_revenue_usd,
        sum(case when order_is_refunded then order_amount_usd else 0 end)
            as total_refunded_usd,

        -- lifecycle dates (all orders)
        min(order_created_at) as first_order_at,
        max(order_created_at) as last_order_at,

        -- lifecycle dates (purchases only)
        min(case when not order_is_failed then order_created_at end)
            as first_purchase_at,
        max(case when not order_is_failed then order_created_at end)
            as last_purchase_at,

        -- time to second purchase (for standardized repeat metrics)
        min(case when customer_purchase_number = 2
            then days_since_first_purchase end) as days_to_second_purchase

    from {{ ref('orders') }}

    {% if is_incremental() %}
    where user_id in (select user_id from customers_to_recompute)
    {% endif %}

    group by 1

)

select
    -- customer grain
    u.user_id,

    -- customer attributes
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

    -- note: only refreshed when the customer is reprocessed —
    -- use last_order_at in BI layer for always-fresh recency
    date_diff(
        current_date(),
        cast(co.last_order_at as date),
        day
    ) as days_since_last_order,

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

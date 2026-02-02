{# Customer-level dimension table aggregated from the orders mart.  #}
{# One row per customer with order counts, revenue, and lifecycle   #}
{# metrics. Refs the orders mart for pre-computed USD amounts and   #}
{# joins user attributes directly from staging.                     #}
{#                                                                  #}
{# Incremental strategy: re-aggregates only customers whose orders  #}
{# changed since the last run (watermarked by last_updated_at).     #}
{# The days_since_last_order column becomes stale for inactive      #}
{# customers; use --full-refresh periodically to correct drift.     #}

{{
    config(
        materialized='incremental',
        unique_key='user_id',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "created_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["platform", "acquisition_channel"]
    )
}}

with {% if is_incremental() %}changed_customers as (

    select distinct src.user_id
    from {{ ref('orders') }} as src
    where src.updated_at > (
        select max(wm.last_updated_at) as _wm
        from {{ this }} as wm
    )

),

{% endif %}order_data as (

    select * from {{ ref('orders') }}

),

users as (

    select * from {{ ref('stg_raw__users') }}

),

countries as (

    select * from {{ ref('seed_countries') }}

),

customer_orders as (

    select
        order_data.user_id,

        -- order counts
        count(*) as total_orders,
        countif(order_data.is_completed) as completed_orders,
        countif(order_data.is_refunded) as refunded_orders,
        countif(order_data.is_failed) as failed_orders,
        countif(not order_data.is_failed) as total_purchases,

        -- revenue
        sum(if(order_data.is_completed, order_data.amount_usd, 0))
            as total_revenue_usd,
        sum(if(order_data.is_refunded, order_data.amount_usd, 0))
            as total_refunded_usd,

        -- dates
        min(order_data.created_at) as first_order_at,
        max(order_data.created_at) as last_order_at,
        min(
            if(
                not order_data.is_failed,
                order_data.created_at, null
            )
        ) as first_purchase_at,
        max(
            if(
                not order_data.is_failed,
                order_data.created_at, null
            )
        ) as last_purchase_at,
        max(order_data.updated_at) as last_updated_at

    from order_data
    {%- if is_incremental() %}
    where order_data.user_id in (
        select changed_customers.user_id
        from changed_customers
    )
{%- endif %}
    group by 1

)

select
    -- ids
    customer_orders.user_id,

    -- user attributes
    users.platform,
    users.acquisition_channel,
    users.ip_country,
    countries.country_name as ip_country_name,
    users.created_at,

    -- order counts
    customer_orders.total_orders,
    customer_orders.completed_orders,
    customer_orders.refunded_orders,
    customer_orders.failed_orders,
    customer_orders.total_purchases,

    -- revenue
    customer_orders.total_revenue_usd,
    customer_orders.total_refunded_usd,

    -- dates
    customer_orders.first_order_at,
    customer_orders.last_order_at,
    customer_orders.first_purchase_at,
    customer_orders.last_purchase_at,
    customer_orders.last_updated_at,

    -- derived
    date_diff(
        customer_orders.last_order_at,
        customer_orders.first_order_at,
        day
    ) as customer_lifetime_days,
    date_diff(
        current_date(),
        date(customer_orders.last_order_at),
        day
    ) as days_since_last_order,
    customer_orders.total_purchases > 1
        as is_repeat_purchaser

from customer_orders
left join users
    on customer_orders.user_id = users.user_id
left join countries
    on users.ip_country = countries.country_code

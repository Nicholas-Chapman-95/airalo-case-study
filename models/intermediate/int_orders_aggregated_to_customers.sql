-- merge strategy: this model is keyed on user_id with no
-- partition (customer-grain, not time-grain). on incremental
-- runs we identify which customers had order changes in the
-- batch, then pull their FULL order history so the aggregations
-- (counts, sums, min/max dates) are recomputed correctly.
-- the merge upserts the recalculated row for each affected
-- customer without touching unchanged customers.
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

-- step 1: find customers affected by this batch
changed_users as (

    -- expects Airflow to pass data_interval_start and
    -- data_interval_end as dbt vars (half-open interval)
    select distinct user_id
    from {{ ref('int_orders_enriched') }}
    where order_updated_at >= timestamp('{{ var("data_interval_start") }}')
      and order_updated_at < timestamp('{{ var("data_interval_end") }}')

),

{% endif %}

-- step 2: pull full order history for affected customers
-- (on full refresh, pull all orders for all customers).
-- aggregations like total_orders and first_order_at need
-- every order to be correct — not just the changed ones.
orders as (

    select
        order_id,
        user_id,
        order_is_completed,
        order_is_refunded,
        order_is_failed,
        order_amount_usd,
        order_created_at,
        order_updated_at
    from {{ ref('int_orders_enriched') }}

    {% if is_incremental() %}
    inner join changed_users
        using (user_id)
    {% endif %}

),

purchase_sequenced as (

    select
        order_id,
        user_id,
        order_is_completed,
        order_is_refunded,
        order_is_failed,
        order_amount_usd,
        order_created_at,
        order_updated_at,
        case
            when not order_is_failed then
                row_number() over (
                    partition by user_id, order_is_failed
                    order by order_created_at, order_id
                )
        end as purchase_number
    from orders

)

select
    user_id,

    -- order counts
    count(distinct order_id) as total_orders,
    count(distinct case when order_is_completed
        then order_id end) as completed_orders,
    count(distinct case when order_is_refunded
        then order_id end) as refunded_orders,
    count(distinct case when order_is_failed
        then order_id end) as failed_orders,
    count(distinct case when not order_is_failed
        then order_id end) as total_purchases,

    -- revenue
    sum(case when order_is_completed
        then order_amount_usd else 0 end)
        as total_revenue_usd,
    sum(case when order_is_refunded
        then order_amount_usd else 0 end)
        as total_refunded_usd,

    -- lifecycle dates (all orders)
    min(order_created_at) as first_order_at,
    max(order_created_at) as last_order_at,

    -- lifecycle dates (purchases only)
    min(case when not order_is_failed
        then order_created_at end)
        as first_purchase_at,
    max(case when not order_is_failed
        then order_created_at end)
        as last_purchase_at,

    -- for incremental change detection downstream
    max(order_updated_at) as last_order_updated_at,

    -- time to second purchase
    date_diff(
        cast(min(case when purchase_number = 2
            then order_created_at end) as date),
        cast(min(case when purchase_number = 1
            then order_created_at end) as date),
        day
    ) as days_to_second_purchase

from purchase_sequenced
group by 1

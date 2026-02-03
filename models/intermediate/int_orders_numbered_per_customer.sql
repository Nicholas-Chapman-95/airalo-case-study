-- merge strategy: the incremental filter is on order_updated_at
-- (when the status changed) but the partition key is order_created_at
-- (when the order was placed). a status change today can affect
-- an order in any historical partition, so we need row-level
-- upserts. insert_overwrite / microbatch would drop and replace
-- entire partitions, destroying sibling rows.
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        partition_by={
            "field": "order_created_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["order_id"],
        on_schema_change='append_new_columns'
    )
}}

with enriched as (

    select
        order_id,
        user_id,
        order_currency,
        order_card_country,
        order_card_country_name,
        order_destination_country,
        order_destination_country_name,
        order_payment_method,
        order_status,
        order_esim_package,
        order_package_data_gigabytes,
        order_package_validity_days,
        order_is_unlimited_package,
        order_is_completed,
        order_is_refunded,
        order_is_failed,
        order_created_at,
        order_updated_at,
        user_signup_platform,
        user_signup_acquisition_channel,
        user_signup_ip_country,
        user_signup_ip_country_name,
        user_signup_at,
        order_amount_local,
        order_amount_usd
    from {{ ref('int_orders_enriched') }}

),

{% if is_incremental() %}

-- step 1: find customers affected by this batch
changed_customers as (

    -- expects Airflow to pass data_interval_start and
    -- data_interval_end as dbt vars (half-open interval)
    select distinct user_id
    from enriched
    where order_updated_at >= timestamp('{{ var("data_interval_start") }}')
      and order_updated_at < timestamp('{{ var("data_interval_end") }}')

),

-- step 2: pull FULL order history for affected customers —
-- window functions need the complete sequence to be correct
orders_to_sequence as (

    select
        order_id,
        user_id,
        order_currency,
        order_card_country,
        order_card_country_name,
        order_destination_country,
        order_destination_country_name,
        order_payment_method,
        order_status,
        order_esim_package,
        order_package_data_gigabytes,
        order_package_validity_days,
        order_is_unlimited_package,
        order_is_completed,
        order_is_refunded,
        order_is_failed,
        order_created_at,
        order_updated_at,
        user_signup_platform,
        user_signup_acquisition_channel,
        user_signup_ip_country,
        user_signup_ip_country_name,
        user_signup_at,
        order_amount_local,
        order_amount_usd
    from enriched
    inner join changed_customers
        using (user_id)

),

{% else %}

orders_to_sequence as (

    select
        order_id,
        user_id,
        order_currency,
        order_card_country,
        order_card_country_name,
        order_destination_country,
        order_destination_country_name,
        order_payment_method,
        order_status,
        order_esim_package,
        order_package_data_gigabytes,
        order_package_validity_days,
        order_is_unlimited_package,
        order_is_completed,
        order_is_refunded,
        order_is_failed,
        order_created_at,
        order_updated_at,
        user_signup_platform,
        user_signup_acquisition_channel,
        user_signup_ip_country,
        user_signup_ip_country_name,
        user_signup_at,
        order_amount_local,
        order_amount_usd
    from enriched

),

{% endif %}

-- step 3: apply window functions over the complete customer
-- order history. all windows are partitioned by user_id so
-- each customer's sequence is independent.
sequenced as (

    select
        *,

        -- attempt number: every order counts, regardless of status
        row_number() over (
            partition by user_id
            order by order_created_at, order_id
        ) as customer_attempt_number,

        -- purchase number: only completed + refunded orders count
        case
            when not order_is_failed then
                row_number() over (
                    partition by user_id, order_is_failed
                    order by order_created_at, order_id
                )
        end as customer_purchase_number,

        -- previous purchase timestamp (within successful orders only)
        case
            when not order_is_failed then
                lag(order_created_at) over (
                    partition by user_id, order_is_failed
                    order by order_created_at, order_id
                )
        end as previous_purchase_at,

        -- first purchase timestamp per customer
        case
            when not order_is_failed then
                min(order_created_at) over (
                    partition by user_id, order_is_failed
                )
        end as customer_first_purchase_at,

        -- prior order history by status (at time of this order)
        countif(order_is_completed) over (
            partition by user_id
            order by order_created_at, order_id
            rows between unbounded preceding and 1 preceding
        ) as customer_prior_completions,

        countif(order_is_refunded) over (
            partition by user_id
            order by order_created_at, order_id
            rows between unbounded preceding and 1 preceding
        ) as customer_prior_refunds,

        countif(order_is_failed) over (
            partition by user_id
            order by order_created_at, order_id
            rows between unbounded preceding and 1 preceding
        ) as customer_prior_failures

    from orders_to_sequence

)

select
    -- order grain
    order_id,

    -- order dimensions
    order_status,
    order_currency,
    order_card_country,
    order_card_country_name,
    order_destination_country,
    order_destination_country_name,
    order_payment_method,
    order_esim_package,
    order_package_data_gigabytes,
    order_package_validity_days,
    order_is_unlimited_package,
    order_is_completed,
    order_is_refunded,
    order_is_failed,
    order_created_at,
    order_updated_at,

    -- customer sequencing
    customer_attempt_number,
    customer_attempt_number = 1 as is_first_attempt,
    customer_purchase_number,
    customer_purchase_number = 1 as is_first_purchase,
    previous_purchase_at,
    date_diff(
        cast(order_created_at as date),
        cast(previous_purchase_at as date),
        day
    ) as days_since_previous_purchase,
    customer_first_purchase_at,
    date_diff(
        cast(order_created_at as date),
        cast(customer_first_purchase_at as date),
        day
    ) as days_since_first_purchase,

    -- customer order history at time of order
    customer_prior_completions,
    customer_prior_refunds,
    customer_prior_failures,

    -- user dimensions
    user_id,
    user_signup_platform,
    user_signup_acquisition_channel,
    user_signup_ip_country,
    user_signup_ip_country_name,
    user_signup_at,

    -- financials
    order_amount_local,
    order_amount_usd

from sequenced

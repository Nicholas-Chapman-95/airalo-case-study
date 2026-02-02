{# Adds two layers of customer sequencing to deduplicated orders: #}
{# 1. Attempt-level: sequences ALL orders (including failed). #}
{# 2. Purchase-level: sequences only completed + refunded orders. #}
{# Failed orders get null purchase columns via the left join. #}

with orders as (

    select * from {{ ref('int_orders_status_resolved') }}

),

attempts as (

    select
        order_id,
        row_number() over (
            partition by user_id
            order by created_at
        ) as customer_attempt_number
    from orders

),

purchases as (

    select
        order_id,
        row_number() over (
            partition by user_id
            order by created_at
        ) as customer_purchase_number,
        lag(created_at) over (
            partition by user_id
            order by created_at
        ) as previous_purchase_at,
        min(created_at) over (
            partition by user_id
        ) as customer_first_purchase_at
    from orders
    where status != 'failed'

)

select
    orders.order_id,
    orders.user_id,
    orders.currency,
    orders.esim_package,
    orders.payment_method,
    orders.card_country,
    orders.destination_country,
    orders.status,
    orders.package_data_gb,
    orders.package_validity_days,
    orders.is_unlimited_package,
    orders.is_completed,
    orders.is_refunded,
    orders.is_failed,
    orders.amount,

    attempts.customer_attempt_number,
    attempts.customer_attempt_number = 1 as is_first_attempt,

    purchases.customer_purchase_number,
    purchases.customer_purchase_number = 1 as is_first_purchase,
    purchases.previous_purchase_at,
    date_diff(
        orders.created_at,
        purchases.previous_purchase_at,
        day
    ) as days_since_previous_purchase,
    purchases.customer_first_purchase_at,
    date_diff(
        orders.created_at,
        purchases.customer_first_purchase_at,
        day
    ) as days_since_first_purchase,

    orders.created_at,
    orders.updated_at

from orders
left join attempts
    on orders.order_id = attempts.order_id
left join purchases
    on orders.order_id = purchases.order_id

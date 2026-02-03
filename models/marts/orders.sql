-- the orders mart is the published BI interface (consumed by
-- Lightdash). all transformation logic — window functions,
-- incremental change detection, customer sequencing — lives
-- in int_orders_numbered_per_customer. this view provides a stable contract
-- layer with column-level metadata and tests defined in _orders.yml.
{{
    config(
        materialized='view'
    )
}}

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
    is_first_attempt,
    customer_purchase_number,
    is_first_purchase,
    previous_purchase_at,
    days_since_previous_purchase,
    customer_first_purchase_at,
    days_since_first_purchase,

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

from {{ ref('int_orders_numbered_per_customer') }}

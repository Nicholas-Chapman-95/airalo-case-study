{# Pre-computed KPIs by acquisition channel. Repeat rates, AOV,     #}
{# realized CLV, repurchase timing, and new-vs-repeat revenue       #}
{# split for executive dashboarding.                                 #}

with customers as (

    select
        acquisition_channel,
        total_purchases,
        total_revenue_usd,
        customer_lifetime_days,
        is_repeat_purchaser
    from {{ ref('customers') }}

),

orders as (

    select
        user_acquisition_channel as acquisition_channel,
        amount_usd,
        is_completed,
        is_first_purchase,
        days_since_previous_purchase
    from {{ ref('orders') }}
    where not is_failed

),

customer_agg as (

    select
        customers.acquisition_channel,

        count(*) as total_customers,
        countif(customers.total_purchases > 0)
            as customers_with_purchase,
        countif(customers.is_repeat_purchaser)
            as repeat_purchasers,

        avg(
            if(customers.total_purchases > 0,
                customers.total_revenue_usd, null)
        ) as avg_revenue_per_customer_usd,

        avg(
            if(customers.total_purchases > 0,
                customers.total_purchases, null)
        ) as avg_orders_per_customer,

        avg(
            if(customers.customer_lifetime_days > 0,
                customers.customer_lifetime_days, null)
        ) as avg_customer_lifetime_days

    from customers
    group by 1

),

order_agg as (

    select
        orders.acquisition_channel,

        avg(if(orders.is_completed, orders.amount_usd, null))
            as avg_order_value_usd,

        avg(orders.days_since_previous_purchase)
            as avg_days_between_purchases,

        approx_quantiles(
            orders.days_since_previous_purchase, 100
        )[offset(50)] as median_days_between_purchases,

        sum(if(orders.is_first_purchase and orders.is_completed,
            orders.amount_usd, 0))
            as first_purchase_revenue_usd,
        sum(if(not orders.is_first_purchase and orders.is_completed,
            orders.amount_usd, 0))
            as repeat_purchase_revenue_usd

    from orders
    group by 1

)

select
    customer_agg.acquisition_channel,

    customer_agg.total_customers,
    customer_agg.customers_with_purchase,
    customer_agg.repeat_purchasers,

    safe_divide(
        customer_agg.repeat_purchasers,
        customer_agg.customers_with_purchase
    ) as repeat_rate,

    order_agg.avg_order_value_usd,
    customer_agg.avg_revenue_per_customer_usd,
    customer_agg.avg_orders_per_customer,

    order_agg.avg_days_between_purchases,
    order_agg.median_days_between_purchases,

    order_agg.first_purchase_revenue_usd,
    order_agg.repeat_purchase_revenue_usd,
    safe_divide(
        order_agg.repeat_purchase_revenue_usd,
        order_agg.first_purchase_revenue_usd
            + order_agg.repeat_purchase_revenue_usd
    ) as pct_revenue_from_repeat,

    customer_agg.avg_customer_lifetime_days

from customer_agg
left join order_agg
    on customer_agg.acquisition_channel
        = order_agg.acquisition_channel

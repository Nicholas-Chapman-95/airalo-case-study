{# Repeat purchase rates segmented by first-purchase eSIM package.   #}
{# Shows overall and time-windowed repeat rates, average revenue,    #}
{# and repurchase timing per package tier.                           #}

with first_purchases as (

    select
        user_id,
        order_esim_package as first_purchase_package,
        order_package_data_gigabytes as first_purchase_data_gb,
        order_package_validity_days as first_purchase_validity_days,
        order_is_unlimited_package as first_purchase_is_unlimited,
        order_amount_usd as first_purchase_amount_usd,
        order_created_at as first_purchase_at

    from {{ ref('orders') }}
    where is_first_purchase

),

second_purchases as (

    select
        user_id,
        order_esim_package as second_purchase_package,
        order_amount_usd as second_purchase_amount_usd,
        days_since_previous_purchase as days_to_second_purchase

    from {{ ref('orders') }}
    where customer_purchase_number = 2

)

select
    fp.first_purchase_package,
    fp.first_purchase_data_gb,
    fp.first_purchase_validity_days,
    fp.first_purchase_is_unlimited,

    -- customer counts
    count(*) as total_customers,
    countif(sp.user_id is not null) as repeat_purchasers,

    -- overall repeat rate
    safe_divide(
        countif(sp.user_id is not null),
        count(*)
    ) as repeat_rate,

    -- time-windowed repeat rates
    safe_divide(
        countif(sp.days_to_second_purchase <= 30),
        count(*)
    ) as repeat_rate_30d,
    safe_divide(
        countif(sp.days_to_second_purchase <= 90),
        count(*)
    ) as repeat_rate_90d,
    safe_divide(
        countif(sp.days_to_second_purchase <= 365),
        count(*)
    ) as repeat_rate_365d,

    -- revenue
    avg(fp.first_purchase_amount_usd) as avg_first_purchase_usd,
    avg(sp.second_purchase_amount_usd) as avg_second_purchase_usd,

    -- repurchase timing
    avg(sp.days_to_second_purchase) as avg_days_to_second_purchase,
    approx_quantiles(
        sp.days_to_second_purchase, 100
    )[offset(50)] as median_days_to_second_purchase

from first_purchases as fp
left join second_purchases as sp
    on fp.user_id = sp.user_id
group by 1, 2, 3, 4
order by total_customers desc

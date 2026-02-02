{# Monthly cohort analysis. One row per signup cohort month,        #}
{# activity month, and acquisition channel. Shows how revenue and   #}
{# customer activity accumulate over time for each cohort, enabling #}
{# LTV projections and channel comparison.                          #}

with orders as (

    select
        user_id,
        user_acquisition_channel,
        user_created_at,
        created_at,
        amount_usd,
        is_completed,
        is_failed
    from {{ ref('orders') }}

),

cohort_activity as (

    select
        date_trunc(orders.user_created_at, month) as signup_cohort_month,
        date_trunc(orders.created_at, month) as activity_month,
        orders.user_acquisition_channel as acquisition_channel,
        orders.user_id,
        orders.amount_usd,
        orders.is_completed,
        orders.is_failed

    from orders

),

cohort_sizes as (

    select
        date_trunc(user_created_at, month) as signup_cohort_month,
        user_acquisition_channel as acquisition_channel,
        count(distinct user_id) as cohort_size

    from orders
    group by 1, 2

),

monthly_metrics as (

    select
        cohort_activity.signup_cohort_month,
        cohort_activity.activity_month,
        cohort_activity.acquisition_channel,

        date_diff(
            cohort_activity.activity_month,
            cohort_activity.signup_cohort_month,
            month
        ) as periods_since_signup,

        count(distinct cohort_activity.user_id) as active_customers,
        count(*) as cohort_orders,
        countif(not cohort_activity.is_failed) as cohort_purchases,

        sum(if(cohort_activity.is_completed, cohort_activity.amount_usd, 0))
            as cohort_revenue_usd

    from cohort_activity
    group by 1, 2, 3

)

select
    monthly_metrics.signup_cohort_month,
    monthly_metrics.activity_month,
    monthly_metrics.acquisition_channel,
    monthly_metrics.periods_since_signup,

    cohort_sizes.cohort_size,

    monthly_metrics.active_customers,
    monthly_metrics.cohort_orders,
    monthly_metrics.cohort_purchases,
    monthly_metrics.cohort_revenue_usd,

    sum(monthly_metrics.cohort_revenue_usd) over (
        partition by
            monthly_metrics.signup_cohort_month,
            monthly_metrics.acquisition_channel
        order by monthly_metrics.activity_month
        rows between unbounded preceding and current row
    ) as cumulative_revenue_usd,

    safe_divide(
        monthly_metrics.active_customers,
        cohort_sizes.cohort_size
    ) as retention_rate,

    safe_divide(
        sum(monthly_metrics.cohort_revenue_usd) over (
            partition by
                monthly_metrics.signup_cohort_month,
                monthly_metrics.acquisition_channel
            order by monthly_metrics.activity_month
            rows between unbounded preceding and current row
        ),
        cohort_sizes.cohort_size
    ) as cumulative_revenue_per_customer

from monthly_metrics
inner join cohort_sizes
    on monthly_metrics.signup_cohort_month
        = cohort_sizes.signup_cohort_month
    and monthly_metrics.acquisition_channel
        = cohort_sizes.acquisition_channel

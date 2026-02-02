{# N-th purchase retention curve. One row per purchase number and    #}
{# acquisition channel showing how many customers reached that       #}
{# purchase step and what proportion continued to the next one.      #}

with purchase_counts as (

    select
        user_id,
        user_acquisition_channel as acquisition_channel,
        max(customer_purchase_number) as max_purchase_number

    from {{ ref('orders') }}
    where not is_failed
    group by 1, 2

),

{# One row per customer per purchase number they reached. #}
purchase_numbers as (

    select
        purchase_counts.user_id,
        purchase_counts.acquisition_channel,
        purchase_number

    from purchase_counts
    cross join unnest(
        generate_array(1, purchase_counts.max_purchase_number)
    ) as purchase_number

),

retention_by_step as (

    select
        purchase_numbers.acquisition_channel,
        purchase_numbers.purchase_number,
        count(distinct purchase_numbers.user_id) as customers_reached

    from purchase_numbers
    group by 1, 2

)

select
    retention_by_step.acquisition_channel,
    retention_by_step.purchase_number,
    retention_by_step.customers_reached,

    lead(retention_by_step.customers_reached) over (
        partition by retention_by_step.acquisition_channel
        order by retention_by_step.purchase_number
    ) as retained_to_next,

    safe_divide(
        lead(retention_by_step.customers_reached) over (
            partition by retention_by_step.acquisition_channel
            order by retention_by_step.purchase_number
        ),
        retention_by_step.customers_reached
    ) as retention_rate

from retention_by_step

{% snapshot snp_raw__exchange_rates %}

{{
    config(
        target_schema='snapshots',
        unique_key='currency',
        strategy='check',
        check_cols=['usd_rate'],
    )
}}

select * from {{ source('raw', 'exchange_rates') }}

{% endsnapshot %}

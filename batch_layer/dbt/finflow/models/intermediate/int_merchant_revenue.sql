{{
    config(
        materialized='table'
    )
}}

/*
  Per-merchant, per-day revenue aggregation.
  Feeds mart_revenue_daily with pre-aggregated merchant metrics.
*/

with stg as (

    select * from {{ ref('stg_transactions') }}

),

merchants as (

    select
        tx_date,
        merchant,
        country_code,
        count(*)                                        as tx_count,
        count(distinct user_id)                         as unique_users,
        sum(amount)                                     as total_revenue_vnd,
        avg(amount)                                     as avg_tx_value_vnd,
        count(*) filter (where is_fraud)                as fraud_tx_count,
        sum(amount) filter (where is_fraud)             as fraud_amount_vnd,
        round(
            100.0 * count(*) filter (where is_fraud)
            / nullif(count(*), 0),
            4
        )                                               as fraud_rate_pct

    from stg
    group by tx_date, merchant, country_code

)

select * from merchants

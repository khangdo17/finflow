{{
    config(
        materialized='table'
    )
}}

/*
  Per-user, per-day aggregation.
  Used by mart_fraud_report to compute user-level fraud rates and amounts.
*/

with stg as (

    select * from {{ ref('stg_transactions') }}

)

select
    tx_date,
    user_id,
    count(*)                                            as total_tx,
    count(*) filter (where is_fraud)                    as fraud_tx,
    sum(amount)                                         as total_amount,
    sum(amount) filter (where is_fraud)                 as fraud_amount,
    avg(amount)                                         as avg_amount,
    max(tx_at)                                          as last_tx_at,
    -- Fraud rate for this user on this day (0 if no transactions)
    round(
        100.0 * count(*) filter (where is_fraud)
        / nullif(count(*), 0),
        2
    )                                                   as daily_fraud_rate_pct

from stg
group by tx_date, user_id

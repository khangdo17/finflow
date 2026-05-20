{{
    config(
        materialized='table'
    )
}}

/*
  Fraud report mart: top flagged users ranked by total fraud transactions.
  Consumed by the FastAPI /fraud/stats endpoint and the Streamlit fraud tab sidebar.
*/

with user_daily as (

    select * from {{ ref('int_user_daily_summary') }}

),

aggregated as (

    select
        user_id,
        sum(fraud_tx)                                   as total_fraud_tx,
        sum(fraud_amount)                               as total_fraud_amount,
        max(last_tx_at)                                 as last_fraud_at,
        sum(total_tx)                                   as total_tx,
        -- Overall fraud rate across all days for this user
        round(
            100.0 * sum(fraud_tx) / nullif(sum(total_tx), 0),
            2
        )                                               as user_fraud_rate_pct

    from user_daily
    where fraud_tx > 0
    group by user_id

)

select
    user_id,
    total_fraud_tx,
    round(total_fraud_amount, 2)                        as total_fraud_amount,
    last_fraud_at,
    user_fraud_rate_pct

from aggregated
order by total_fraud_tx desc

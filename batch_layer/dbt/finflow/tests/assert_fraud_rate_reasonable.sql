/*
  Custom test: verify the overall fraud rate in mart_revenue_daily
  stays between 0.1% and 20%. Must return 0 rows to pass.
  A rate outside this band signals either a broken generator or a bad dbt run.
*/

with overall as (

    select
        sum(tx_count)       as total_tx,
        sum(fraud_tx_count) as total_fraud_tx,
        round(
            100.0 * sum(fraud_tx_count) / nullif(sum(tx_count), 0),
            4
        )                   as overall_fraud_rate_pct
    from {{ ref('mart_revenue_daily') }}

)

select overall_fraud_rate_pct
from overall
where overall_fraud_rate_pct < 0.1
   or overall_fraud_rate_pct > 20.0

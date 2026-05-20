{{
    config(
        materialized='table'
    )
}}

/*
  Daily revenue mart: one row per (date, merchant, country).
  Joins merchant revenue with a static category lookup derived from the source data.
  Consumed by the FastAPI /revenue/daily and /revenue/by-merchant endpoints.
*/

with merchant_rev as (

    select * from {{ ref('int_merchant_revenue') }}

),

-- Derive category from stg_transactions since it comes from the generator profiles
stg as (

    select
        tx_date,
        merchant,
        -- Take any non-null fraud_reason as indicator; category comes from the source field
        -- We embed merchant→category mapping here as a static lookup
        case merchant
            when 'Grab'      then 'transport'
            when 'Shopee'    then 'ecommerce'
            when 'VinMart'   then 'grocery'
            when 'MoMo'      then 'fintech'
            when 'Tiki'      then 'ecommerce'
            when 'Highlands' then 'food'
            when 'Circle K'  then 'grocery'
            when 'ZaloPay'   then 'fintech'
            when 'VNPay'     then 'fintech'
            when 'Lazada'    then 'ecommerce'
            else 'other'
        end                                             as category
    from {{ ref('stg_transactions') }}
    group by tx_date, merchant

)

select
    mr.tx_date                                          as date,
    mr.merchant,
    s.category,
    mr.country_code,
    mr.tx_count,
    mr.unique_users,
    mr.total_revenue_vnd,
    round(mr.avg_tx_value_vnd, 2)                       as avg_tx_value_vnd,
    mr.fraud_tx_count,
    mr.fraud_rate_pct,
    coalesce(mr.fraud_amount_vnd, 0)                    as fraud_amount_vnd

from merchant_rev mr
left join stg s
    on mr.tx_date = s.tx_date
    and mr.merchant = s.merchant

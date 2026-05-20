{{
    config(
        materialized='view'
    )
}}

/*
  Staging layer: clean, cast, and rename raw_transactions.
  Drops records with null tx_id or null tx_at as they cannot be keyed or windowed downstream.
  Truncates tx_at to milliseconds to avoid sub-ms precision issues in aggregations.
*/

with source as (

    select * from {{ source('finflow_raw', 'raw_transactions') }}

),

cleaned as (

    select
        tx_id                                           as tx_id,
        trim(user_id)                                   as user_id,
        trim(merchant)                                  as merchant,
        amount::numeric(15, 2)                          as amount,
        upper(trim(currency))                           as currency,
        upper(trim(country))                            as country_code,
        date_trunc('millisecond', tx_at)                as tx_at,
        lower(trim(device))                             as device,
        coalesce(is_fraud, false)                       as is_fraud,
        fraud_reason                                    as fraud_reason,
        ingested_at                                     as ingested_at,
        tx_at::date                                     as tx_date

    from source
    where tx_id is not null
      and tx_at  is not null
      and amount > 0

)

select * from cleaned

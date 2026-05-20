/*
  Custom test: verify no negative amounts exist in stg_transactions.
  Must return 0 rows to pass. Negative amounts indicate data corruption upstream.
*/

select
    tx_id,
    amount
from {{ ref('stg_transactions') }}
where amount < 0

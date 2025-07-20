{{ config(
    enabled = false
) }}

 select * from public.marts_reconcile_raw_transactions_vs_psp_transactions


select *
from public_raw.transaction_data t
left join public_raw.psp_transactions p
    on t.transaction_id = p.original_transaction_id
    and t.currency = p.psp_currency
    and t.amount = p.psp_amount
where p.original_transaction_id is not null


ROLLBACK;
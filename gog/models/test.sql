{{ config(
    enabled = false
) }}

select sum(amount_pln) from public.successful_transactions
where transaction_date between '2025-07-01' and '2025-07-02'

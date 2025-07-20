{{ config(
    materialized = 'view'
) }}

with transactions as (
    select
        transaction_id,
        user_id,
        game_id,
        transaction_date,
        amount,
        currency,
        payment_method,
        product_type
    from {{ref('stg_transactions')}}
),

psp_transactions as (
    select
        psp_transaction_id,
        original_transaction_id,
        psp_amount,
        psp_currency,
        psp_timestamp,
        status
    from {{ref('stg_psp_transactions')}}
),

valid_products_transactions as (
    select
        t.transaction_id,
        t.user_id,
        t.game_id,
        t.transaction_date,
        t.amount,
        t.currency,
        t.payment_method,
        t.product_type,
        g.game_key,
        g.game_title,
        g.genre,
        g.developer,
        g.release_date
    from {{ref('stg_transactions')}} t
    left join {{ref('dim_games')}} g
        on t.game_id = g.game_key
),

transactions_joined as (
    select
        t.transaction_id,
        t.amount,
        t.currency,
        t.payment_method,
        t.game_id,
        t.game_key,
        psp.psp_transaction_id,
        psp.original_transaction_id,
        psp.psp_amount,
        psp.psp_currency
    from valid_products_transactions t
    full outer join psp_transactions psp
        on t.transaction_id = psp.original_transaction_id
),

final as(
    select
        count(tj.transaction_id) as transactions_all_records,
        COUNT(tj.transaction_id) FILTER (WHERE tj.game_key IS NOT NULL) AS transaction_records_with_valid_products,
        COUNT(tj.transaction_id) FILTER (WHERE tj.game_key IS NULL) AS transaction_records_with_invalid_products,
        count(tj.psp_transaction_id) as psp_transactions_all_records,
        COUNT(tj.transaction_id) FILTER (WHERE tj.game_key IS NOT NULL and tj.transaction_id is not null and tj.psp_transaction_id is not null) AS recognised_psp_transactions,
        COUNT(tj.transaction_id) FILTER (WHERE tj.game_key IS NOT NULL and tj.transaction_id is not null and tj.psp_transaction_id is not null and tj.currency = tj.psp_currency) AS recognised_psp_transactions_with_matching_currency,
        COUNT(tj.transaction_id) FILTER (WHERE tj.game_key IS NOT NULL and tj.transaction_id is not null and tj.psp_transaction_id is not null and tj.currency = tj.psp_currency and tj.amount = tj.psp_amount) AS recognised_psp_transactions_with_matching_currency_and_value,
        COUNT(tj.transaction_id) FILTER (WHERE tj.game_key IS NOT NULL and tj.transaction_id is not null and tj.psp_transaction_id is not null and tj.currency != tj.psp_currency and tj.amount != tj.psp_amount) AS recognised_psp_transactions_with_non_matching_currency_and_value,
        COUNT(tj.transaction_id) FILTER (WHERE tj.game_key IS NOT NULL and tj.psp_transaction_id is null) AS not_recognised_transactions,
        COUNT(tj.psp_transaction_id) FILTER (WHERE tj.game_key IS NOT NULL and tj.transaction_id is null) AS not_recognised_psp_transactions
    from transactions_joined tj )

select
    transactions_all_records,
    transaction_records_with_valid_products,
    transaction_records_with_invalid_products,
    psp_transactions_all_records,
    recognised_psp_transactions,
    recognised_psp_transactions_with_matching_currency,
    recognised_psp_transactions_with_matching_currency_and_value,
    recognised_psp_transactions_with_non_matching_currency_and_value,
    not_recognised_transactions,
    not_recognised_psp_transactions
from final
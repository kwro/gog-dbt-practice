{{ config(
    materialized = 'table'
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
    from {{ ref('stg__transactions') }}
),

psp_transactions as (
    select
        psp_transaction_id,
        original_transaction_id,
        psp_amount,
        psp_currency,
        psp_timestamp,
        status
    from {{ ref('stg__psp_transactions') }}
),

dim_games as (
    select
        game_key,
        game_title,
        genre,
        developer,
        release_date
    from {{ ref('dim__games') }}
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
    from transactions as t
    left join dim_games as g
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
    from valid_products_transactions as t
    full outer join psp_transactions as psp
        on t.transaction_id = psp.original_transaction_id
),

final as (
    select
        count(tj.transaction_id) as transactions_all_records,
        count(tj.transaction_id) filter (where tj.game_key is not NULL) as transaction_records_with_valid_products,
        count(tj.transaction_id) filter (where tj.game_key is NULL) as transaction_records_with_invalid_products,
        count(tj.psp_transaction_id) as psp_transactions_all_records,
        count(tj.transaction_id) filter (
            where tj.game_key is not NULL and tj.transaction_id is not NULL and tj.psp_transaction_id is not NULL
        ) as recognised_psp_transactions,
        count(tj.transaction_id) filter (
            where tj.game_key is not NULL
            and tj.transaction_id is not NULL
            and tj.psp_transaction_id is not NULL
            and tj.currency = tj.psp_currency
        ) as recognised_psp_transactions_with_matching_currency,
        count(tj.transaction_id) filter (
            where tj.game_key is not NULL
            and tj.transaction_id is not NULL
            and tj.psp_transaction_id is not NULL
            and tj.currency = tj.psp_currency
            and tj.amount = tj.psp_amount
        ) as recognised_psp_transactions_with_matching_currency_and_value,
        count(tj.transaction_id) filter (
            where tj.game_key is not NULL
            and tj.transaction_id is not NULL
            and tj.psp_transaction_id is not NULL
            and tj.currency != tj.psp_currency
            and tj.amount != tj.psp_amount
        ) as recognised_psp_transactions_with_non_matching_currency_and_value,
        count(tj.transaction_id) filter (
            where tj.game_key is not NULL and tj.psp_transaction_id is NULL
        ) as not_recognised_transactions,
        count(tj.psp_transaction_id) filter (
            where tj.game_key is not NULL and tj.transaction_id is NULL
        ) as not_recognised_psp_transactions
    from transactions_joined as tj
)

select
    transactions_all_records,
    psp_transactions_all_records,
    recognised_psp_transactions,
    transaction_records_with_valid_products,
    transaction_records_with_invalid_products,
    recognised_psp_transactions_with_matching_currency,
    recognised_psp_transactions_with_matching_currency_and_value,
    recognised_psp_transactions_with_non_matching_currency_and_value,
    not_recognised_transactions,
    not_recognised_psp_transactions
from final

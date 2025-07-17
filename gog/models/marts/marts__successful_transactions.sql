with successful_psp as (
    select
        psp_transaction_id,
        original_transaction_id as transaction_id,
        psp_amount,
        psp_currency,
        psp_timestamp
    from {{ ref('stg_psp_transactions') }}
    where status = 'SUCCESS'
),

exchange_rates as (
    select
        "date" as exchange_date,
        currency_from,
        rate as exchange_rate
    from {{ ref('stg_daily_exchange_rates') }}
    where currency_to = 'PLN'
),

transactions as (
    select
        t.transaction_id,
        t.user_id,
        t.game_id,
        t.transaction_date,
        t.amount,
        t.currency,
        t.payment_method,
        t.product_type
    from {{ ref('stg_transactions') }} t
),

joined as (
    select
        t.transaction_id,
        t.user_id,
        t.game_id,
        t.transaction_date,
        t.amount,
        t.currency,
        t.payment_method,
        t.product_type,
        psp.psp_amount,
        psp.psp_currency,
        psp.psp_timestamp,
        e.exchange_rate,
        t.amount * CASE WHEN t.currency = 'PLN' THEN 1 ELSE e.exchange_rate END AS amount_pln
    from transactions t
    inner join successful_psp psp
        on t.transaction_id = psp.transaction_id
    left join exchange_rates e
        on t.transaction_date = e.exchange_date
        and t.currency = e.currency_from
),

final as (
    select
        transaction_id,
        user_id,
        game_id,
        transaction_date,
        payment_method,
        product_type,
        amount_pln
    from joined
)

select * from final
order by transaction_date DESC

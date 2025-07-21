with successful_psp as (
    select
        psp_transaction_id,
        original_transaction_id as transaction_id,
        psp_amount,
        psp_currency,
        psp_timestamp
    from {{ ref('stg__psp_transactions') }}
    where status = 'SUCCESS'
),

exchange_rates as (
    select
        "date" as exchange_date,
        currency_from,
        rate as exchange_rate
    from {{ ref('stg__daily_exchange_rates') }}
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
    from {{ ref('stg__transactions') }} as t
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
        t.amount * case when t.currency = 'PLN' then 1 else e.exchange_rate end as amount_pln,
        g.game_title,
        g.genre,
        g.developer,
        g.release_date
    from transactions as t
    inner join successful_psp as psp
        on t.transaction_id = psp.transaction_id
    inner join dim_games as g
        on t.game_id = g.game_key
    left join exchange_rates as e
        on
            t.transaction_date = e.exchange_date
            and t.currency = e.currency_from
)

select
    transaction_id,
    user_id,
    game_id,
    genre,
    game_title,
    transaction_date,
    amount,
    currency,
    payment_method,
    product_type,
    amount_pln,
    release_date
from joined
order by 4 desc
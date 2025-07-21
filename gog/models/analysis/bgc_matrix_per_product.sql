{{ config(materialized = 'view') }}

with transactions as (
    select
        transaction_id,
        user_id,
        game_id,
        transaction_date,
        payment_method,
        product_type,
        amount_pln,
        amount,
        currency
    from {{ ref('marts__successful_transactions') }}
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
        t.payment_method,
        t.product_type,
        t.amount_pln,
        g.game_title,
        g.genre,
        g.developer,
        g.release_date
    from transactions as t
    left join dim_games as g
        on t.game_id = g.game_key
),

-- Filter last 24 months
filtered as (
    select *
    from joined
    where transaction_date >= (current_date - interval '24 months')
),

aggregated as (
    select
        game_id,
        game_title,
        genre,
        product_type,
        date_trunc('month', transaction_date) as transaction_month,
        sum(amount_pln) as total_amount_pln
    from filtered
    group by
        1, 2, 3, 4, 5
),

market_growth as (
    select
        game_id,
        game_title,
        genre,
        product_type,
        sum(case when transaction_month >= (current_date - interval '12 months') then total_amount_pln else 0 end) as amount_last_12m,
        sum(case when transaction_month < (current_date - interval '12 months') then total_amount_pln else 0 end) as amount_prev_12m,
        sum(total_amount_pln) as total_amount_last_24m
    from aggregated
    group by
        1,2,3,4
),

market_totals as (
    select
        sum(total_amount_pln) as total_market_sales_24m
    from aggregated
),

final as (
    select
        mg.game_id,
        mg.game_title,
        mg.genre,
        mg.product_type,
        mg.amount_last_12m,
        mg.amount_prev_12m,
        case
            when mg.amount_prev_12m = 0 then null
            else (mg.amount_last_12m / mg.amount_prev_12m) - 1
        end as market_growth_pct,
        mg.total_amount_last_24m,
        total_market_sales_24m,
        mg.total_amount_last_24m / nullif(mt.total_market_sales_24m, 0) as market_share_pct
    from market_growth as mg
    cross join market_totals as mt
)

select * from final

{% set core_products = var('core_products') %}
{% set addon_products = var('addon_products') %}

with transactions as (
    select
        transaction_id,
        user_id,
        game_id,
        transaction_date,
        payment_method,
        product_type,
        amount_pln
    from {{ref('marts__successful_transactions')}}
    where transaction_date between (select min("date") from {{ref('stg_daily_exchange_rates')}})
        and (select max("date") from {{ref('stg_daily_exchange_rates')}})
),

user_segments as (
    select
        user_id,
        count(distinct game_id) as distinct_games,
        count(*) as total_transactions,
        sum(amount_pln) as total_amount_pln,
        sum(case when product_type in ({{ "'" ~ core_products | join("','") ~ "'" }}) then 1 else 0 end) as purchases_base,
        sum(case when product_type in ({{ "'" ~ addon_products | join("','") ~ "'" }}) then 1 else 0 end) as purchases_expansion
    from transactions
    group by user_id
),
days_between_transactions as (
    select
        user_id,
        transaction_date,
        lag(transaction_date) over (partition by user_id order by transaction_date) as previous_transaction_date
    from transactions
),

average_days_between as (
    select
        user_id,
        avg(transaction_date - previous_transaction_date) as avg_days_between_purchases
    from days_between_transactions
    group by user_id
)

select s.*, a.avg_days_between_purchases
from user_segments s
left join average_days_between a
    on s.user_id = a.user_id
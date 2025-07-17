{{ config(
    enabled = false
) }}

with


user_purchases as (
    select
        user_id,
        transaction_id,
        transaction_date::date as transaction_date,
        amount,
        product_type
    from {{ ref('stg_transactions') }}
),
SELECT
    transaction_id,
    user_id,
    game_id,
    transaction_date,
    amount,
    currency,
    payment_method,
    product_type
FROM {{ source('gog_raw', 'raw_transactions') }}
WHERE transaction_id IS NOT NULL

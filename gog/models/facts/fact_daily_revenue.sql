{{ config(
    materialized='incremental',
    unique_key=['transaction_date', 'product_type', 'genre', 'payment_method']
) }}

WITH transactions AS (
    SELECT
        transaction_id,
        user_id,
        game_id,
        transaction_date,
        payment_method,
        product_type,
        amount_pln
    FROM {{ ref('marts__successful_transactions') }}
    {% if is_incremental() %}
       WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

games AS (
    SELECT
        game_key,
        genre
    FROM {{ ref('dim_games') }}
),

transactions_with_genre AS (
    SELECT
        t.transaction_date,
        t.product_type,
        g.genre,
        t.payment_method,
        t.amount_pln
    FROM transactions t
    LEFT JOIN games g
        ON t.game_id = g.game_key
),

daily_revenue AS (
    SELECT
        transaction_date,
        product_type,
        genre,
        payment_method,
        SUM(amount_pln) AS total_revenue_pln
    FROM transactions_with_genre
    GROUP BY 1, 2, 3, 4
)

SELECT
    transaction_date,
    product_type,
    genre,
    payment_method,
    total_revenue_pln
FROM daily_revenue
ORDER BY transaction_date, product_type, genre, payment_method

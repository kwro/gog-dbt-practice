SELECT
    "date",
    currency_from,
    currency_to,
    CAST(rate AS NUMERIC(10, 4)) AS rate
FROM {{ source('gog_raw', 'exchange_rates') }}
WHERE "date" IS NOT NULL

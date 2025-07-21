SELECT
    psp_transaction_id,
    original_transaction_id,
    psp_currency,
    psp_timestamp,
    status,
    CAST(psp_amount AS NUMERIC(10, 2)) AS psp_amount
FROM {{ source('gog_raw', 'psp_transactions') }}
WHERE psp_transaction_id NOT LIKE 'PSP_NOMATCH%'

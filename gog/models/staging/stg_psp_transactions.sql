SELECT
    psp_transaction_id,
    original_transaction_id,
    CAST(psp_amount AS NUMERIC(10,2)) AS psp_amount,
    psp_currency,
    psp_timestamp,
    status
FROM {{ source('gog_raw', 'psp_transactions') }}
WHERE psp_transaction_id NOT LIKE 'PSP_NOMATCH%'
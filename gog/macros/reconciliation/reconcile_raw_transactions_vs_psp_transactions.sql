{% macro reconcile_raw_transactions_vs_psp_transactions() %}
    {% if execute %}
        {% set transactions = ref('stg_transactions') %}
        {% set psp_transactions = ref('stg_psp_transactions') %}
        {% set unmatched_raw = run_query("SELECT COUNT(*) FROM " ~ transactions ~ " WHERE transaction_id NOT IN (SELECT original_transaction_id FROM " ~ psp_transactions ~ " WHERE status = 'SUCCESS' and original_transaction_id IS NOT NULL)") %}
        {% set count_raw = unmatched_raw.columns[0].values()[0] %}
        {{ log("RAW transactions not in PSP: " ~ count_raw, info=True) }}

        {% set unmatched_psp = run_query("SELECT COUNT(*) FROM " ~ psp_transactions ~ " WHERE status = 'SUCCESS' AND original_transaction_id IS NOT NULL AND original_transaction_id NOT IN (SELECT transaction_id FROM " ~ transactions ~ ")") %}
        {% set count_psp = unmatched_psp.columns[0].values()[0] %}
        {{ log("PSP transactions not in RAW: " ~ count_psp, info=True) }}
    {% endif %}
{% endmacro %}

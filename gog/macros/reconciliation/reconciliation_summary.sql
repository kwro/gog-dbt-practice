-- depends_on: {{ ref('marts__reconcile_raw_transactions_vs_psp_transactions') }}
{% macro reconciliation_summary() %}
    {% set model_name = ref('marts__reconcile_raw_transactions_vs_psp_transactions') %}
    {% if execute %}

        {% set query = "SELECT * FROM " ~ model_name %}
        {% set results = run_query(query) %}

        {% do log("Reconciliation summary:", info=True) %}
        {% set columns = results.column_names %}
        {% set row = results.rows[0] %}
        {% for col, val in zip(columns, row) %}
            {% do log(col ~ ": " ~ val, info=True) %}
        {% endfor %}
    {% endif %}
{% endmacro %}

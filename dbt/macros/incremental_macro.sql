{% macro apply_incremental_filter(source_table,partition_field) %}
    SELECT *
    FROM {{ source_table }}
    {% if is_incremental() %}
        WHERE TIMESTAMP_TRUNC({{ partition_field }}, DAY) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    {% else %}
        WHERE {{ partition_field }} > TIMESTAMP("1900-01-01 00:00:00")
    {% endif %}
{% endmacro %}

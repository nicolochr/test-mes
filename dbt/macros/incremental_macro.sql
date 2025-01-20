{% macro apply_incremental_filter(partition_field) %}
    {% if is_incremental() %}
        TIMESTAMP_TRUNC({{ partition_field }}, DAY) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    {% else %}
        {{ partition_field }} > TIMESTAMP("1900-01-01 00:00:00")
    {% endif %}
{% endmacro %}

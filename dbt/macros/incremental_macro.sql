{% macro incremental_logic(source_table) %}
    {% if is_incremental() %}
        SELECT *
        FROM source_table 
        WHERE _PARTITIONTIME >= TIMESTAMP_SUB(
            (SELECT MAX(_PARTITIONTIME) FROM {{ this }}),
            INTERVAL 1 DAY)
    {% else %}
        SELECT *
        FROM  source_table 
    {% endif %}
{% endmacro %}

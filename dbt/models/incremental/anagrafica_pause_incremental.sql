{{
  config(
    materialized = 'incremental',
    unique_key = 'pk',
    incremental_strategy='merge',
    post_hook="TRUNCATE TABLE `kafka_landing.{{ var('table_name_anagraficapause') }}`"
    )
}}

WITH kafka_data AS (
    SELECT
        CAST(JSON_VALUE(payload, "$.pk") AS INT64) AS pk,
        CAST(JSON_VALUE(payload, "$.endDate") AS STRING) AS enddate_value,
        CAST(JSON_VALUE(payload, "$.startDate") AS STRING) AS startdate_value,
        CAST(JSON_VALUE(payload, "$.startValidity") AS TIMESTAMP)
            AS startvalidity_tstamp,
        CAST(JSON_VALUE(payload, "$.endValidity") AS TIMESTAMP)
            AS endvalidity_tstamp,
        CAST(kafka_meta.inserttime AS TIMESTAMP)
            AS inserttime_tstamp
    FROM {{ source('kafka_landing', var('table_name_anagraficapause')) }}
),

new_data AS (
    {% if is_incremental() %}
        SELECT *
        FROM kafka_data
        WHERE
            kafka_data.inserttime_tstamp
            > (SELECT MAX(inserttime_tstamp) FROM {{ this }})
    {% else %}
        SELECT *
        FROM kafka_data
    {% endif %}
),

dedupped AS (
    SELECT *
    FROM new_data
    WHERE TRUE
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY pk ORDER BY inserttime_tstamp DESC) = 1
)

SELECT *
FROM dedupped

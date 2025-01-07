{{
  config(
    materialized = 'incremental',
    unique_key = 'pk',
    incremental_strategy = 'merge',
    post_hook="TRUNCATE TABLE `kafka_landing.{{ var('table_name_userlog') }}`"
  )
}}

WITH kafka_data AS (
    SELECT
        CAST(JSON_VALUE(payload, "$.pk") AS INT64) AS pk,
        CAST(JSON_VALUE(payload, "$.creationDate") AS TIMESTAMP)
            AS creationdate_tstamp,
        CAST(JSON_VALUE(payload, "$.action") AS STRING) AS action_type,
        CAST(JSON_VALUE(payload, "$.userPK") AS INT64) AS user_pk,
        CAST(JSON_VALUE(payload, "$.postazione") AS STRING) AS postazione_code,
        CAST(kafka_meta.inserttime AS TIMESTAMP) AS inserttime_tstamp
    FROM {{ source('kafka_landing',  var('table_name_userlog')) }}
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
    WHERE true
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY pk ORDER BY inserttime_tstamp DESC) = 1
)

SELECT *
FROM dedupped

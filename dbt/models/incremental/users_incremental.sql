{{
  config(
    materialized='incremental',
    unique_key='pk',
    incremental_strategy='merge',
    post_hook="TRUNCATE TABLE `kafka_landing.{{ var('table_name_users') }}`"
  )
}}

WITH kafka_data AS (
    SELECT
        CAST(JSON_VALUE(payload, "$.pk") AS INT64) AS pk,
        CAST(JSON_VALUE(payload, "$.email") AS STRING) AS email_desc,
        CAST(JSON_VALUE(payload, "$.name") AS STRING) AS name_,
        CAST(JSON_VALUE(payload, "$.surname") AS STRING) AS surname,
        CAST(JSON_VALUE(payload, "$.username") AS STRING) AS username,
        CAST(JSON_VALUE(payload, "$.placePk") AS INT64) AS place_pk,
        CAST(JSON_VALUE(payload, "$.userId") AS INT64) AS user_id,
        CAST(kafka_meta.inserttime AS TIMESTAMP) AS inserttime_tstamp
    FROM {{ source('kafka_landing', var('table_name_users')) }}
),

new_data AS (
    {% if is_incremental() %}
        SELECT *
        FROM kafka_data
        WHERE kafka_data.inserttime_tstamp > (
            SELECT MAX(inserttime_tstamp)
            FROM {{ this }}
        )
    {% else %}
        SELECT *
        FROM kafka_data
    {% endif %}
),

dedupped AS (
    SELECT *
    FROM new_data
    WHERE TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY inserttime_tstamp DESC) = 1
)

SELECT *
FROM dedupped

{{
  config(
    materialized = 'incremental',
    unique_key = 'pk',
    incremental_strategy = 'merge'
  )
}}

WITH kafka_data AS (
    SELECT
        PARSE_JSON(payload)                     AS parsed_payload,
        CAST(kafka_meta.insertTime AS TIMESTAMP) AS inserttime_tstamp
    FROM {{ source('kafka_landing',  var('table_name_userlog')) }}
),

parsed_data AS (
    SELECT
        CAST(parsed_payload.data.pk AS INT64)               AS pk,
        CAST(parsed_payload.data.creationdate AS TIMESTAMP) AS creationdate_tstamp,
        CAST(parsed_payload.data.action AS STRING)          AS action_type,
        CAST(parsed_payload.data.userpk AS INT64)           AS user_pk,
        CAST(parsed_payload.data.postazione AS STRING)      AS postazione_code,
        CAST(parsed_payload.data.insertTime AS TIMESTAMP)   AS inserttime_tstamp
    FROM kafka_data
),

new_data AS (
    {% if is_incremental() %}
        SELECT *
        FROM parsed_data
        WHERE _partitiontime >= TIMESTAMP_SUB(
            (SELECT MAX(_partitiontime) FROM {{ this }}),
            INTERVAL 1 DAY
        )
    {% else %}
        SELECT *
        FROM  parsed_data 
    {% endif %}
),

dedupped AS (
    SELECT *
    FROM new_data
    WHERE true
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY pk
            ORDER BY inserttime_tstamp DESC
        ) = 1
)

SELECT *
FROM dedupped

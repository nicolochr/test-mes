{{
  config(
    materialized = 'incremental',
    unique_key = 'pk',
    incremental_strategy='merge'
    )
}}


WITH kafka_data AS (
    SELECT
        PARSE_JSON(payload)                     AS parsed_payload,
        CAST(kafka_meta.insertTime AS TIMESTAMP) AS inserttime_tstamp
    FROM {{ source('kafka_landing', var('table_name_interruzioni')) }}
),

parsed_data AS (
    SELECT
        CAST(parsed_payload.data.pk AS INT64)             AS pk,
        CAST(parsed_payload.data.enddate AS TIMESTAMP)    AS enddate_tstamp,
        CAST(parsed_payload.data.startdate AS TIMESTAMP)
            AS startdate_tstamp,
        CAST(parsed_payload.data.type AS STRING)          AS type_name,
        CAST(parsed_payload.data.lavorazione AS INT64)    AS lavorazione_pk,
        CAST(parsed_payload.data.userid AS INT64)         AS userpk_code,
        CAST(parsed_payload.data.insertTime AS TIMESTAMP)
            AS inserttime_tstamp
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
    WHERE TRUE
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY pk
            ORDER BY inserttime_tstamp DESC
        ) = 1
)

SELECT *
FROM dedupped

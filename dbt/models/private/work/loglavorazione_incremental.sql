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
    FROM {{ source('kafka_landing', var('table_name_loglavorazione')) }}
),

parsed_data AS (
    SELECT
        CAST(parsed_payload.data.pk AS INT64)                         AS pk,
        CAST(parsed_payload.data.actualtime AS FLOAT64)               AS actualtime_qty,
        CAST(parsed_payload.data.codicebarratura AS STRING)
            AS codicebarratura_code,
        CAST(parsed_payload.data.codicemacrofase AS STRING)
            AS codicemacrofase_code,
        CAST(parsed_payload.data.codicemotivoeconomia AS STRING)
            AS codicemotivoeconomia_code,
        CAST(parsed_payload.data.codiceoperazione AS STRING)
            AS codiceoperazione_code,
        CAST(parsed_payload.data.datamatrix AS STRING)                AS datamatrix_code,
        CAST(parsed_payload.data.descrizionebarratura AS STRING)
            AS descrizionebarratura_desc,
        CAST(parsed_payload.data.descrizionemacrofase AS STRING)      AS descrizionemacrofase_desc,
        CAST(parsed_payload.data.descrizionemotivoeconomia AS STRING)
            AS descrizionemotivoeconomia_desc,
        CAST(parsed_payload.data.descrizioneoperazione AS STRING)     AS descrizioneoperazione_desc,
        CAST(parsed_payload.data.enddate AS TIMESTAMP)                AS enddate_tstamp,
        CAST(parsed_payload.data.moltiplicatore AS FLOAT64)           AS moltiplicatore_num,
        CAST(parsed_payload.data.startdate AS TIMESTAMP)              AS startdate_tstamp,
        CAST(parsed_payload.data.status AS STRING)                    AS status_type,
        CAST(parsed_payload.data.totaltime AS FLOAT64)                AS totaltime_qty,
        CAST(parsed_payload.data.userpk AS INT64)                     AS userpk_code,
        CAST(parsed_payload.data.ordercode AS STRING)                 AS ordercode_code,
        CAST(parsed_payload.data.postazione AS STRING)                AS postazione_code,
        CAST(parsed_payload.data.codicereparto AS STRING)             AS codicereparto_code,
        CAST(parsed_payload.data.descrizionereparto AS STRING)        AS descrizionereparto_desc,
        CAST(parsed_payload.data.centrolavorocode AS STRING)          AS centrolavorocode_code,
        CAST(parsed_payload.data.stabilimentocode AS STRING)          AS stabilimentocode_code,
        CAST(parsed_payload.data.repartocode AS STRING)               AS repartocode_code,
        CAST(parsed_payload.data.insertTime AS TIMESTAMP)             AS inserttime_tstamp
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

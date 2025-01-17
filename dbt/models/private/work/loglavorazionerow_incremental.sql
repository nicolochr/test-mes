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
    FROM {{ source('kafka_landing',  var('table_name_loglavorazionerow')) }}
),

parsed_data AS (
    SELECT
        CAST(parsed_payload.data.creationdate AS TIMESTAMP)
            AS creationdate_tstamp,
        CAST(parsed_payload.data.pk AS INT64)                  AS pk,
        CAST(parsed_payload.data.codiceciclo AS STRING)
            AS codiceciclo_code,
        CAST(parsed_payload.data.conformation AS STRING)       AS conformation_type,
        CAST(parsed_payload.data.cuttingorder AS STRING)       AS cuttingorder_code,
        CAST(parsed_payload.data.productcode AS STRING)        AS productcode_code,
        CAST(parsed_payload.data.productdescription AS STRING)
            AS productdescription_desc,
        CAST(parsed_payload.data.progressive AS FLOAT64)       AS progressive_num,
        CAST(parsed_payload.data.size AS STRING)               AS size_val,
        CAST(parsed_payload.data.tempimedi AS FLOAT64)         AS tempimedi_qty,
        CAST(parsed_payload.data.totaltime AS FLOAT64)         AS totaltime_qty,
        CAST(parsed_payload.data.loglavorazionepk AS INT64)    AS log_lavorazione_pk,
        CAST(parsed_payload.data.insertTime AS TIMESTAMP)      AS inserttime_tstamp,
        CAST(parsed_payload.data.status AS STRING)             AS status_type
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

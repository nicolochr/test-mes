{{
  config(
    materialized = 'incremental',
    unique_key = 'pk',
    incremental_strategy='merge',
    post_hook="TRUNCATE TABLE `kafka_landing.{{ var('table_name_loglavorazionerow') }}`"
    )
}}

WITH kafka_data AS (
    SELECT
        CAST(JSON_VALUE(payload, '$.creationdate') AS TIMESTAMP)
            AS creationdate_tstamp,
        CAST(JSON_VALUE(payload, '$.pk') AS INT64) AS pk,
        CAST(JSON_VALUE(payload, '$.codiceCiclo') AS STRING)
            AS codiceciclo_code,
        CAST(JSON_VALUE(payload, '$.conformation') AS STRING)
            AS conformation_type,
        CAST(JSON_VALUE(payload, '$.cuttingOrder') AS STRING)
            AS cuttingorder_code,
        CAST(JSON_VALUE(payload, '$.productCode') AS STRING)
            AS productcode_code,
        CAST(JSON_VALUE(payload, '$.productDescription') AS STRING)
            AS productdescription_desc,
        CAST(JSON_VALUE(payload, '$.progressive') AS FLOAT64)
            AS progressive_num,
        CAST(JSON_VALUE(payload, '$.size') AS STRING) AS size_val,
        CAST(JSON_VALUE(payload, '$.tempiMedi') AS FLOAT64) AS tempimedi_qty,
        CAST(JSON_VALUE(payload, '$.totalTime') AS FLOAT64) AS totaltime_qty,
        CAST(JSON_VALUE(payload, '$.logLavorazionePk') AS INT64)
            AS log_lavorazione_pk,
        CAST(kafka_meta.inserttime AS TIMESTAMP) AS inserttime_tstamp,
        CAST(JSON_VALUE(payload, '$.status') AS STRING) AS status_type
    FROM {{ source('kafka_landing',  var('table_name_loglavorazionerow')) }}
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

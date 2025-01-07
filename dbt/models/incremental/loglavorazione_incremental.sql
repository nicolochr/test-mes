{{
  config(
    materialized = 'incremental',
    unique_key = 'pk',
    incremental_strategy = 'merge',
    post_hook="TRUNCATE TABLE `kafka_landing.{{ var('table_name_loglavorazione') }}`"
  )
}}

WITH kafka_data AS (
    SELECT
        CAST(JSON_VALUE(payload, '$.pk') AS INT64) AS pk,
        CAST(JSON_VALUE(payload, '$.actualTime') AS FLOAT64) AS actualtime_qty,
        CAST(JSON_VALUE(payload, '$.codiceBarratura') AS STRING)
            AS codicebarratura_code,
        CAST(JSON_VALUE(payload, '$.codiceMacrofase') AS STRING)
            AS codicemacrofase_code,
        CAST(JSON_VALUE(payload, '$.codiceMotivoEconomia') AS STRING)
            AS codicemotivoeconomia_code,
        CAST(JSON_VALUE(payload, '$.codiceOperazione') AS STRING)
            AS codiceoperazione_code,
        CAST(JSON_VALUE(payload, '$.datamatrix') AS STRING) AS datamatrix_code,
        CAST(JSON_VALUE(payload, '$.descrizioneBarratura') AS STRING)
            AS descrizionebarratura_desc,
        CAST(JSON_VALUE(payload, '$.descrizioneMacrofase') AS STRING)
            AS descrizionemacrofase_desc,
        CAST(JSON_VALUE(payload, '$.descrizioneMotivoEconomia') AS STRING)
            AS descrizionemotivoeconomia_desc,
        CAST(JSON_VALUE(payload, '$.descrizioneOperazione') AS STRING)
            AS descrizioneoperazione_desc,
        CAST(JSON_VALUE(payload, '$.endDate') AS TIMESTAMP) AS enddate_tstamp,
        CAST(JSON_VALUE(payload, '$.moltiplicatore') AS FLOAT64)
            AS moltiplicatore_num,
        CAST(JSON_VALUE(payload, '$.startDate') AS TIMESTAMP)
            AS startdate_tstamp,
        CAST(JSON_VALUE(payload, '$.status') AS STRING) AS status_type,
        CAST(JSON_VALUE(payload, '$.totalTime') AS FLOAT64) AS totaltime_qty,
        CAST(JSON_VALUE(payload, '$.userPk') AS INT64) AS userpk_code,
        CAST(JSON_VALUE(payload, '$.orderCode') AS STRING) AS ordercode_code,
        CAST(JSON_VALUE(payload, '$.postazione') AS STRING) AS postazione_code,
        CAST(JSON_VALUE(payload, '$.codiceReparto') AS STRING)
            AS codicereparto_code,
        CAST(JSON_VALUE(payload, '$.descrizioneReparto') AS STRING)
            AS descrizionereparto_desc,
        CAST(JSON_VALUE(payload, '$.centroLavoroCode') AS STRING)
            AS centrolavorocode_code,
        CAST(JSON_VALUE(payload, '$.stabilimentoCode') AS STRING)
            AS stabilimentocode_code,
        CAST(JSON_VALUE(payload, '$.repartoCode') AS STRING)
            AS repartocode_code,
        CAST(kafka_meta.inserttime AS TIMESTAMP) AS inserttime_tstamp
    FROM {{ source('kafka_landing', var('table_name_loglavorazione')) }}
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

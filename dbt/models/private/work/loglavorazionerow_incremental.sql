{{
  config(
    materialized = 'incremental',
    unique_key = 'pk',
    incremental_strategy='merge',
    partition_by = {
        'field': 'partitiontime',
        'data_type': 'timestamp',
        'granularity':'day'
        }
    )
}}

WITH kafka_data AS (
    SELECT
        CAST(JSON_VALUE(payload, '$.creationdate') AS TIMESTAMP)
            AS creationdate_tstamp,
        CAST(JSON_VALUE(payload, '$.pk') AS INT64)                  AS pk,
        CAST(JSON_VALUE(payload, '$.mezzoCapo') AS STRING)          AS mezzocapo,
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
        CAST(JSON_VALUE(payload, '$.size') AS STRING)               AS size_val,
        CAST(JSON_VALUE(payload, '$.tempiMedi') AS FLOAT64)         AS tempimedi_qty,
        CAST(JSON_VALUE(payload, '$.totalTime') AS FLOAT64)         AS totaltime_qty,
        CAST(JSON_VALUE(payload, '$.logLavorazionePk') AS INT64)
            AS log_lavorazione_pk,
        CAST(kafka_meta.inserttime AS TIMESTAMP)                    AS inserttime_tstamp,
        CAST(JSON_VALUE(payload, '$.status') AS STRING)             AS status_type,
        CAST(_partitiontime AS TIMESTAMP)                           AS partitiontime
    FROM {{ source('kafka_landing',  var('table_name_loglavorazionerow')) }}
),

new_data AS (
    {{ apply_incremental_filter(
        source_table='kafka_data',
        partition_field='partitiontime'
    ) }}
),

dedupped AS (
    {{ dbt_utils.deduplicate(
      relation='new_data',
      partition_by='pk',
      order_by='inserttime_tstamp desc',
     )
  }}
)

SELECT *
FROM dedupped

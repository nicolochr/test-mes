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
        CAST(JSON_VALUE(payload, "$.pk") AS INT64)            AS pk,
        CAST(JSON_VALUE(payload, "$.endDate") AS TIMESTAMP)   AS enddate_tstamp,
        CAST(JSON_VALUE(payload, "$.startDate") AS TIMESTAMP)
            AS startdate_tstamp,
        CAST(JSON_VALUE(payload, "$.type") AS STRING)         AS type_name,
        CAST(JSON_VALUE(payload, "$.lavorazione") AS INT64)   AS lavorazione_pk,
        CAST(JSON_VALUE(payload, "$.userId") AS INT64)        AS userpk_code,
        CAST(kafka_meta.inserttime AS TIMESTAMP)              AS inserttime_tstamp,
        CAST(_partitiontime AS TIMESTAMP)                     AS partitiontime
    FROM {{ source('kafka_landing', var('table_name_interruzioni')) }}
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

{{
  config(
    materialized='incremental',
    unique_key='pk',
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
        CAST(JSON_VALUE(payload, "$.pk") AS INT64)        AS pk,
        CAST(JSON_VALUE(payload, "$.email") AS STRING)    AS email_desc,
        CAST(JSON_VALUE(payload, "$.name") AS STRING)     AS name_,
        CAST(JSON_VALUE(payload, "$.surname") AS STRING)  AS surname,
        CAST(JSON_VALUE(payload, "$.username") AS STRING) AS username,
        CAST(JSON_VALUE(payload, "$.placePk") AS INT64)   AS place_pk,
        CAST(JSON_VALUE(payload, "$.userId") AS INT64)    AS user_id,
        CAST(kafka_meta.inserttime AS TIMESTAMP)          AS inserttime_tstamp,
        CAST(_partitiontime AS TIMESTAMP)                 AS partitiontime
    FROM {{ source('kafka_landing', var('table_name_users')) }}
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

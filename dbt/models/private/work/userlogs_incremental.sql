{{
  config(
    materialized = 'incremental',
    unique_key = 'pk',
    incremental_strategy = 'merge',
    partition_by = {
        'field': 'partitiontime',
        'data_type': 'timestamp',
        'granularity':'day'
        }
  )
}}

WITH kafka_data AS (
    SELECT
        CAST(JSON_VALUE(payload, "$.pk") AS INT64)               AS pk,
        CAST(JSON_VALUE(payload, "$.creationDate") AS TIMESTAMP)
            AS creationdate_tstamp,
        CAST(JSON_VALUE(payload, "$.action") AS STRING)          AS action_type,
        CAST(JSON_VALUE(payload, "$.userPK") AS INT64)           AS user_pk,
        CAST(JSON_VALUE(payload, "$.postazione") AS STRING)      AS postazione_code,
        CAST(kafka_meta.inserttime AS TIMESTAMP)                 AS inserttime_tstamp,
        CAST(_partitiontime AS TIMESTAMP)                        AS partitiontime
    FROM {{ source('kafka_landing',  var('table_name_userlog')) }}
),

new_data AS (
    SELECT *
    FROM kafka_data
    WHERE {{ apply_incremental_filter('partitiontime') }}
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

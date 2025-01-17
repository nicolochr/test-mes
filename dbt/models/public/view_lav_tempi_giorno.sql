{{
  config(
    materialized = 'view',
    schema = 'gold_layer'
    )
}}

SELECT *
FROM {{ ref('view_lav_tempi_giorno_private') }}

{{
  config(
    materialized = 'view',
    schema = 'gold_layer'
    )
}}

SELECT *
FROM {{ ref('view_lav_rilevazioni_private') }}

{{ 
    config(
        materialized = 'view'
    ) 
}}

SELECT *
FROM {{ ref('view_lav_addetti_private') }}

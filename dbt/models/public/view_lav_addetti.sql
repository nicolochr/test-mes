{{ 
    config(
        materialized = 'view'
    ) 
}}

WITH renamed AS (
    SELECT
        'TEMERA' AS source_name,
        '' AS ditta_name,
        user_id AS addetto_code,
        surname AS cognome,
        name_ AS nome,
        '' AS matricola_code,
        '' AS cdc_dipendente_code,
        '' AS cdc_descrizione_desc,
        pk,
        '' AS matricola_name
    FROM {{ ref('users_incremental') }}
    WHERE user_id IS NOT NULL
)

SELECT *
FROM renamed

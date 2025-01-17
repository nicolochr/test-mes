{{
  config(
    materialized = 'view',
    schema = 'gold_layer'
    )
}}

WITH renamed AS (
    SELECT
        "TEMERA"                      AS source_,
        au.user_id                    AS addetto,
        ll.codicemotivoeconomia_code  AS codice_economia,
        DATE(ll.startdate_tstamp)     AS data_,
        SUM(ll.actualtime_qty) / 60.0 AS tempo
    FROM
        {{ ref('loglavorazione_incremental') }} AS ll
    INNER JOIN {{ ref('users_incremental') }} AS au
        ON ll.userpk_code = au.pk
    WHERE
        ll.status_type = "COMPLETED"
        AND au.user_id IS NOT NULL
    GROUP BY
        au.user_id,
        DATE(ll.startdate_tstamp),
        ll.codicemotivoeconomia_code
)

SELECT *
FROM renamed

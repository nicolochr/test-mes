{{
  config(
    severity = "error"
    )
}}

WITH checktable AS (
  SELECT 
  COUNT(`pk`) AS _Count,
  COUNT(DISTINCT `pk`) AS _DistinctCount
  FROM  {{ ref('users_incremental') }}
),

countchecktable AS (
  SELECT 
  COUNT(*) AS _CountCheck
  FROM checktable
  WHERE _Count <> _DistinctCount
)

SELECT *
FROM countchecktable
WHERE _CountCheck <> 0
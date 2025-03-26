{{ config(
    materialized='table'
) }}


SELECT f.*
FROM {{ref("fact_leads")}} f
WHERE EXISTS (
    SELECT 1
    FROM {{source("gorgias_growth", "fact_data_similar_web")}} sw
    WHERE sw.website = f.domain
    AND sw.total_visits IS NOT NULL
)
LIMIT 200

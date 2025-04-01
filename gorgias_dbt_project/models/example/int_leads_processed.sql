{{ config(
    materialized='table'
) }}

WITH leads_staging AS (
    SELECT DISTINCT
        CAST(domain AS STRING) AS domain,
        CAST(ecommerce_platform AS STRING) AS ecommerce_platform,
        CAST(helpdesk AS STRING) AS helpdesk,
        SPLIT(technologies_app_partners, ';') AS technologies_app_partners,
        CAST(estimated_gmv_band AS STRING) AS estimated_gmv_band
    FROM {{ref("stg_leads")}}
)

SELECT * 
FROM leads_staging

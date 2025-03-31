{{ config(
    materialized='table'
) }}

WITH leads_enriched AS (
  SELECT *
  FROM {{source("gorgias_growth", "dim_leads_clay_enriched")}}
),

similarweb_enrichment AS (
  SELECT *
  FROM {{source("gorgias_growth", "dim_leads_similarweb_metrics")}}
),

customers_enriched AS (
  SELECT *
  FROM {{source("gorgias_growth", "dim_customers_clay_enriched")}}
),

similar_customers AS (
    -- Looking if one of our customers is in the list of similar websites from our leads
  SELECT similarweb_enrichment.website AS lead_domain,
          ce.name AS customer_company_name
  FROM similarweb_enrichment,
  UNNEST(similar_websites.website) AS sim_websites
  LEFT JOIN customers_enriched ce
    ON sim_websites = ce.domain
  WHERE ce.domain IS NOT NULL

)

SELECT le.domain,
      INITCAP(le.ecommerce_platform) AS ecommerce_platform,
      INITCAP(le.helpdesk) AS helpdesk,
      ARRAY_TO_STRING(
            ARRAY(SELECT INITCAP(app)
            FROM Unnest(split(REPLACE(REPLACE(REPLACE(le.technologies_app_partners, '[', ''), ']', ''), '_', ' '), ',')) AS app
      ), ", ") AS technologies_app_partners, -- Cleaning the list and aggregating them as a String
      le.estimated_gmv_band,
      COALESCE(le.Name, SPLIT(le.domain, '.')[SAFE_OFFSET(0)]) AS company_name, --Using the name from the enrichment in Clay, else taking the name by cleaning the domain
      le.size AS company_size,
      COALESCE(le.industry, swe.industry) AS industry, --Taking the industry either from CLay enrichment or similarweb data
      COALESCE(le.description, swe.description) AS description,
      le.type AS company_type,
      COALESCE(le.country, swe.country) AS country,
      le.locality,
      le.main_product,
      swe.total_visits AS total_visits_last_3_months,
      swe.visits_change_last_month,
      swe.pages_per_visit,
      swe.bounce_rate, --If it is high, it can be interesting to help them reduce it
      ARRAY_TO_STRING(swe.similar_websites.website, ", ") AS similar_websites, --Keeping the similar websites as a String to make the format easier for other tools
      STRING_AGG(ce_product.Name, ", ") OVER(PARTITION BY le.domain) AS customer_with_same_product, --Using window function to capture all the customers in case there are several with the same product
      STRING_AGG(ce_industry.Name, ", ") OVER(PARTITION BY le.domain) AS customer_with_same_industry,
      STRING_AGG(sc.customer_company_name, ", ") OVER(PARTITION BY le.domain) AS customer_with_similar_website

FROM leads_enriched le
LEFT JOIN similarweb_enrichment swe
  ON le.domain = swe.website
LEFT JOIN customers_enriched ce_product --Joining customers if they have the same product as the lead, as this can be used in the email as customer success story
  ON le.Main_Product = ce_product.Main_Product
LEFT JOIN customers_enriched ce_industry --Same as with the product
  ON ce_industry.Industry = le.Industry
    AND (ce_industry.estimated_gmv_band = le.estimated_gmv_band
          OR ce_industry.country = le.country)
LEFT JOIN similar_customers sc
  ON le.domain = sc.lead_domain

QUALIFY ROW_NUMBER() OVER(PARTITION BY le.domain) = 1 --Making sure there are no duplicates
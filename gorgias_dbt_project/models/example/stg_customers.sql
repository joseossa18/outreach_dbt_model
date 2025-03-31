CREATE OR REPLACE EXTERNAL TABLE `gorgias-case-study-454321.gorgias_growth.customers_staging`
(
  domain STRING,
  ecommerce_platform STRING,
  helpdesk STRING,
  technologies_app_partners STRING,
  estimated_gmv_band STRING,
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://growth_storage/customers.csv'],
  skip_leading_rows = 1
);
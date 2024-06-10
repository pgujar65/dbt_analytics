
{{ config(schema=generate_schema_name('HARMONIZATION'),
             materialized='table'
            )}}

WITH sr_detail AS (
  SELECT * 
  FROM 
  --{{ source('RAW_DATA', 'in_wallbay_sr_detail') }}
   {{ ref('in_wallbay_sr_detail') }}
  WHERE civil != 'CSD'
),
wallbayScanReportData AS (
  SELECT * 
  FROM --{{ source('RAW_DATA', 'in_wallbay_scan_zylem') }}
   {{ ref('in_wallbay_scan_zylem') }}
),
srDetailDataSelected AS ( 
  SELECT DISTINCT b.region, a.state, a.city, a.sales_representative_name, a.asset_no, a.customer_id, a.licence_no, a.wallbay_description
  FROM sr_detail a 
  LEFT JOIN (SELECT DISTINCT region, state FROM wallbayScanReportData) b 
  ON a.state = b.state
),
srDetailAssign AS (
  SELECT region, state, city, sales_representative_name, COUNT(asset_no) AS total_wb 
  FROM srDetailDataSelected
  GROUP BY 1, 2, 3, 4
)
SELECT DISTINCT * 
FROM srDetailAssign
JOIN srDetailDataSelected
USING (region, state, city, sales_representative_name)


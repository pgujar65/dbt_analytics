
{{
  config(materialized = "table",schema=generate_schema_name('HARMONIZATION'))
}}
WITH sr_detail AS (
select * 
  FROM {{ source('RAW_DATA', 'in_wallbay_sr_detail') }}
where civil != "CSD"
),
wallbayScanReportData AS (
  SELECT * 
  FROM {{ source('RAW_DATA', 'in_wallbay_scan_zylem') }}
),
srDetailDataSelected AS ( 
  SELECT DISTINCT b.region, a.state, a.city, a.sales_representative_name, a.asset_no, a.customer_id, a.licence_no, a.wallbay_description
  FROM sr_detail a 
  LEFT JOIN (SELECT DISTINCT region, state FROM wallbayScanReportData) b 
  ON a.state = b.state
),
wallbayScanDupsRemove AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY asset_no, sales_representative_name, asset_no, date ORDER BY time) AS rw_no 
  FROM wallbayScanReportData
)
SELECT *,
     
       CASE
         WHEN EXTRACT(day FROM date) BETWEEN 1 AND 8 THEN 'Week1'
         WHEN EXTRACT(day FROM date) BETWEEN 9 AND 15 THEN 'Week2'
         WHEN EXTRACT(day FROM date) BETWEEN 16 AND 22 THEN 'Week3'
         ELSE 'Week4'
       END AS week
FROM wallbayScanDupsRemove
WHERE rw_no = 1

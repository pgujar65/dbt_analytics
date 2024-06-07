
{{ config(schema='REPORTING', materialized='table') }}

SELECT DISTINCT sr.region, sr.state, sr.city, sr.sales_representative_name, sr.licence_no, sr.wallbay_description, sr.total_wb, sr.asset_no, sr.customer_id, wb.date, wb.time, wb.latitude, wb.longitude, wb.remark, wb.civil, wb.week,
       CASE 
         WHEN wb.outlet IS NULL THEN wb.outlet_name 
         ELSE wb.outlet 
       END AS outlet
FROM RAW_DATA_HARMONIZATION.sr_detail_asset_join sr 
LEFT JOIN RAW_DATA_HARMONIZATION.wallbay_scan_date wb 
USING (asset_no, customer_id, sales_representative_name)
LEFT JOIN (SELECT DISTINCT customer_id, outlet AS outlet_name FROM RAW_DATA_HARMONIZATION.wallbay_scan_date) AS o
USING (customer_id)

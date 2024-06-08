
{{ config(schema=generate_schema_name('REPORTING'),
             materialized='table',
           pre_hook = "
                SELECT 1 AS Dummy FROM {{ ref('wallbay_scan_date') }};
                SELECT 1 AS Dummy FROM {{ ref('sr_detail_asset_join') }};
            "

            )}}

SELECT DISTINCT sr.region, sr.state, sr.city, sr.sales_representative_name, sr.licence_no, sr.wallbay_description, sr.total_wb, sr.asset_no, sr.customer_id, wb.date, wb.time, wb.latitude, wb.longitude, wb.remark, wb.civil, wb.week,
       CASE 
         WHEN wb.outlet IS NULL THEN wb.outlet_name 
         ELSE wb.outlet 
       END AS outlet
FROM {{ source('HARMONIZATION', 'sr_detail_asset_join') }} sr
LEFT JOIN  {{ source('HARMONIZATION', 'wallbay_scan_date') }} wb
USING (asset_no, customer_id, sales_representative_name)
LEFT JOIN (SELECT DISTINCT customer_id, outlet AS outlet_name FROM RAW_DATA_HARMONIZATION.wallbay_scan_date) AS o
USING (customer_id)

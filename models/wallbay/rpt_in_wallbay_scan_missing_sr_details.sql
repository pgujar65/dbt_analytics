

{{ config(schema=generate_schema_name('REPORTING'),
             materialized='table',
            pre_hook = "
                SELECT 1 AS Dummy FROM {{ ref('rpt_in_wallbay_scan_report') }};
            "
            )}}

SELECT 
    a.region, 
    a.state, 
    a.city, 
    a.sales_representative_name, 
    a.asset_no, 
    MAX(a.date) AS last_scan_date
FROM 
     {{ source('HARMONIZATION', 'wallbay_scan_date') }} a 
LEFT JOIN 
     {{ source('HARMONIZATION', 'sr_detail_asset_join') }} AS b 

ON 
    a.asset_no = b.asset_no 
    AND a.customer_id = b.customer_id 
    AND a.sales_representative_name = b.sales_representative_name
WHERE 
    b.asset_no IS NULL 
    AND b.customer_id IS NULL 
    AND b.sales_representative_name IS NULL
GROUP BY 
    a.region, 
    a.state, 
    a.city, 
    a.sales_representative_name, 
    a.asset_no
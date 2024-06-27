{{ config(schema=generate_schema_name('GDH_US_VIEW'),
             materialized='view',

            )}}

SELECT
con.market_key AS store_key,
st.store_id,
con.product_key,
prd.beam_product_id,
prd.upc,
prd.manufacturer,
con.calendar_key,
date,
SUM(units) AS units,
SUM(dol) AS dol,
SUM(actual_cases) AS actual_cases,
SUM(standard_cases) AS standard_cases,
AVG(fob_price) AS fob_price,
AVG(shelf_price) AS shelf_price,
AVG(retail_price) AS retail_price,
SUM(eq_9l) AS eq_9l

FROM 
{{ source('GDH_GOLD', 'fact_consumption_weekly') }} as con

LEFT JOIN 
{{ source('GDH_GOLD', 'dim_calendar') }} as cal
ON con.calendar_key = cal.calendar_key

LEFT JOIN 
{{ source('GDH_US_VIEW', 'dim_product_nabca_sam') }} as prd
ON con.product_key = prd.product_key

LEFT JOIN 
 {{ source('GDH_US_VIEW', 'dim_store_nabca_sam') }} as st
ON con.market_key = st.store_key
 
WHERE con.src_sys_nm = "US NABCA SAM"

GROUP BY 1,2,3,4,5,6,7,8
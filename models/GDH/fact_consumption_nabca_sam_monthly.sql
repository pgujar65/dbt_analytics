{{ config(schema=generate_schema_name('GDH_REPORTING'),
             materialized='table',
           pre_hook = "
                SELECT 1 AS Dummy FROM {{ ref('dim_product') }};
                SELECT 1 AS Dummy FROM {{ ref('dim_store') }};
                SELECT 1 AS Dummy FROM {{ ref('fact_consumption_monthly') }};
            "

            )}}


SELECT
con.product_key,
con.market_key as store_key,
con.calendar_key,
cal.date,
SUM(units) AS units,
SUM(dol) AS dol,
SUM(actual_cases) AS actual_cases,
SUM(standard_cases) AS standard_cases,
AVG(fob_price) AS fob_price,
AVG(shelf_price) AS shelf_price,
AVG(retail_price) AS retail_price,
SUM(eq_9l) AS eq_9l

FROM 
--{{ source('GDH_GOLD', 'fact_consumption_monthly') }}
{{ ref('fact_consumption_monthly') }} con

LEFT JOIN 
--{{ source('GDH_GOLD', 'dim_product') }}
{{ ref('dim_product') }} prd
ON con.product_key=prd.product_key

LEFT JOIN 
--{{ source('GDH_GOLD', 'dim_store') }}
{{ ref('dim_store') }}  st
ON con.market_key=st.store_key

LEFT JOIN 
{{ source('GDH_GOLD', 'dim_calendar') }} cal
ON con.calendar_key=cal.calendar_key

GROUP BY 1,2,3,4

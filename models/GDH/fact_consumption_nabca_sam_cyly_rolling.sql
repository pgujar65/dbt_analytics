{{ config(schema=generate_schema_name('GDH_REPORTING'),
             materialized='table',
           pre_hook = "
                SELECT 1 AS Dummy FROM {{ ref('fact_consumption_nabca_sam_monthly_cyly') }};
                  "

            )}}

SELECT 
product_key,
store_key,
date
units_cy,
        {{ rolling_sum('units_cy', 'date',2)  }} as units_cy_3m,
        {{ rolling_sum('units_cy', 'date', 5) }} as units_cy_6m,
        {{ rolling_sum('units_cy', 'date', 11) }} as units_cy_12m,
        {{ rolling_sum('units_cy', 'date', 1000000) }} as units_cy_ytd,
units_ly,
        {{ rolling_sum('units_ly', 'date', 2) }} as units_ly_3m,
        {{ rolling_sum('units_ly', 'date', 5) }} as units_ly_6m,
        {{ rolling_sum('units_ly', 'date', 11) }} as units_ly_12m,
        {{ rolling_sum('units_ly', 'date', 1000000) }} as units_ly_ytd,
dol_cy,
        {{ rolling_sum('dol_cy', 'date', 2) }} as dol_cy_3m,
        {{ rolling_sum('dol_cy', 'date', 5) }} as dol_cy_6m,
        {{ rolling_sum('dol_cy', 'date', 11) }} as dol_cy_12m,
        {{ rolling_sum('dol_cy', 'date', 1000000) }} as dol_cy_ytd,
dol_ly,
        {{ rolling_sum('dol_ly', 'date', 2) }} as dol_ly_3m,
        {{ rolling_sum('dol_ly', 'date', 5) }} as dol_ly_6m,
        {{ rolling_sum('dol_ly', 'date', 11) }} as dol_ly_12m,
        {{ rolling_sum('dol_ly', 'date', 1000000) }} as dol_ly_ytd,
FROM 
--GDH_REPORTING.fact_consumption_nabca_sam_monthly_cyly
 {{ ref('fact_consumption_nabca_sam_monthly_cyly') }}
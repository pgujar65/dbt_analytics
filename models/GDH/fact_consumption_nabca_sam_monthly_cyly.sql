{{ config(schema=generate_schema_name('GDH_REPORTING'),
             materialized='table',
           pre_hook = "
    
                SELECT 1 AS Dummy FROM {{ ref('fact_consumption_nabca_sam_monthly') }};
            "
            )}}

select 
    product_key,
    store_key,
    date,
    units as units_cy,
     {{ get_last_year_value('date', 'units') }} as units_ly,
     dol as dol_cy,
     {{ get_last_year_value('date', 'dol') }} as dol_ly,
     FROM 
    -- `GDH_REPORTING.fact_consumption_nabca_sam_monthly`
     {{ ref('fact_consumption_nabca_sam_monthly') }} con



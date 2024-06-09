{{ config(schema=generate_schema_name('GDH_GOLD'),
             materialized='table',

            )}}

select *  
FROM {{ source('GDH_SILVER', 'fact_consumption_monthly') }}
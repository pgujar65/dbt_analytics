{{ config(schema=generate_schema_name('REPORTING'),
             materialized='table',
           pre_hook = "
                SELECT 1 AS Dummy FROM {{ ref('dim_product') }};
                SELECT 1 AS Dummy FROM {{ ref('dim_store') }};
            "

            )}}

select *  
FROM {{ source('GDH_GOLD', 'dim_product') }}
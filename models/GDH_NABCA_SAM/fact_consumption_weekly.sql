{{ config(schema=generate_schema_name('GDH_GOLD'),
             materialized='table',

            )}}

SELECT
	*
FROM {{ source('GDH_SILVER', 'fact_consumption_weekly') }} as s
WHERE rec_crt_dt IS NOT NULL

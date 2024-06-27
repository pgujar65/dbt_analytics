{{ config(schema=generate_schema_name('GDH_US_CONSM'),
             materialized='table',

            )}}

WITH consumptionMonthly as 
(
SELECT con.* except(bq_load_timestamp), 
date
    FROM 
    {{ source('GDH_GOLD', 'fact_consumption_monthly') }} as con
    INNER JOIN (select distinct product_key 
                FROM 
                {{ source('GDH_US_VIEW', 'dim_product_nabca_sam') }}
                WHERE alcohol_type = "SPIRIT"
                ) prd 
    ON con.product_key = prd.product_key
    LEFT JOIN (
                select distinct calendar_key , date
                FROM 
                {{ source('GDH_GOLD', 'dim_calendar') }} as cal
                ) cal
    ON con.calendar_key = cal.calendar_key
    WHERE con.src_sys_nm = "US NABCA SAM"

),
ComplemetryTableCalculation AS (
    SELECT market_key,product_key,date,
    sum(units) as units,
    sum(dol) as dol,
    sum(actual_cases) as actual_cases,
    sum(standard_cases) as standard_cases,
    sum(eq_9l) as eq_9l,
    sum(fob_price) as fob_price,
    sum(shelf_price) as shelf_price,
    sum(retail_price) as retail_price
    FROM (
        SELECT 
           market_key,
                product_key,	
                a.date,
            0.0 AS units,
            0.0 AS dol,
            0.0 AS actual_cases,
            0.0 AS standard_cases,
            0.0 AS eq_9l,
            0.0 AS fob_price,
            0.0 AS shelf_price,
            0.0 AS retail_price
        FROM consumptionMonthly
        CROSS JOIN (SELECT DISTINCT date FROM consumptionMonthly) a
        GROUP BY 1,2,3
    UNION ALL 

     SELECT 
           market_key,
           product_key,	
           date,
            units,
            dol,
            actual_cases,
            standard_cases,
            eq_9l,
            fob_price,
            shelf_price,
            retail_price
        FROM consumptionMonthly
    )
    group by 1,2,3
    ),
consMonthlyCy as (
SELECT 
    market_key as store_key,
    product_key,
    date,
    SUM(units) AS units_cy,
    SUM(dol) AS dol_cy,
    SUM(actual_cases) AS actual_cases_cy,
    SUM(standard_cases) AS standard_cases_cy,
    SUM(eq_9l) AS eq_9l_cy,
    AVG(fob_price) AS fob_price_cy,
    AVG(shelf_price) AS shelf_price_cy,
    AVG(retail_price) AS retail_price_cy
FROM consumptionMonthly
WHERE extract(year from date) >= (SELECT MAX(extract(year from date)) AS maxYear FROM consumptionMonthly) - 6
GROUP BY 1,2,3
),
consMonthlyLy as (
SELECT 
    market_key as store_key,
    product_key,
    LAST_DAY(DATE_ADD(date, INTERVAL 12 MONTH)) AS date,
    SUM(units) AS units_ly,
    SUM(dol) AS dol_ly,
    SUM(actual_cases) AS actual_cases_ly,
    SUM(standard_cases) AS standard_cases_ly,
    SUM(eq_9l) AS eq_9l_ly,
    AVG(fob_price) AS fob_price_ly,
    AVG(shelf_price) AS shelf_price_ly,
    AVG(retail_price) AS retail_price_ly
FROM consumptionMonthly
WHERE extract(year from date) >= (SELECT MAX(extract(year from date)) AS maxYear FROM consumptionMonthly) - 6
GROUP BY 1,2,3
)
select * 
FROM consMonthlyCy cy 
JOIN consMonthlyLy ly 
using(store_key,product_key,date)


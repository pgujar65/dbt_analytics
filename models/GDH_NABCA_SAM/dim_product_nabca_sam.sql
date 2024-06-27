{{ config(schema=generate_schema_name('GDH_US_VIEW'),
             materialized='view',

            )}}

SELECT
prd.product_key
,beam_product_key AS beam_product_id
,short_product_desc
,alcohol_type
,brand_extension
,brand_category
,brand_subcategory
,segment
,brand_family
,age
,bottle_per_case
,proof
,size_in_ml
,size
,upc
,manufacturer
,brand_no
,brand_abrev
,class_no
,class_abrev
,com_code
,com_cname
,vendor_no
,vendor
,vendor_abrev
,bg_vendor
,status_ind
,scc_code
,pack_size_ind
,imp_dom_flag
,brand_full
,SAFE_CAST(SUBSTR(upc,1,LENGTH(upc)-1) AS NUMERIC) AS upc_int

FROM {{ source('GDH_GOLD', 'dim_product') }} as prd

LEFT JOIN
(
SELECT
product_key
,MAX(CASE WHEN attribute_master_id = "4001" THEN attribute_code END) AS brand_no
,MAX(CASE WHEN attribute_master_id = "4001" THEN attribute_desc END) AS brand_abrev
,MAX(CASE WHEN attribute_master_id = "4002" THEN attribute_code END) AS class_no
,MAX(CASE WHEN attribute_master_id = "4002" THEN attribute_desc END) AS class_abrev
,MAX(CASE WHEN attribute_master_id = "4003" THEN attribute_code END) AS com_code
,MAX(CASE WHEN attribute_master_id = "4003" THEN attribute_desc END) AS com_cname
,MAX(CASE WHEN attribute_master_id = "4004" THEN attribute_code END) AS vendor_no
,MAX(CASE WHEN attribute_master_id = "4004" THEN attribute_desc END) AS vendor_abrev
,MAX(CASE WHEN attribute_master_id = "4005" THEN attribute_code END) AS status_ind
,MAX(CASE WHEN attribute_master_id = "4006" THEN attribute_code END) AS scc_code
,MAX(CASE WHEN attribute_master_id = "4007" THEN attribute_code END) AS pack_size_ind
,MAX(CASE WHEN attribute_master_id = "4008" THEN attribute_code END) AS imp_dom_flag
,MAX(CASE WHEN attribute_master_id = "4009" THEN attribute_code END) AS brand_full
,MAX(CASE WHEN attribute_master_id = "4010" THEN attribute_code END) AS vendor
,MAX(CASE WHEN attribute_master_id = "4011" THEN attribute_code END) AS bg_vendor

FROM
{{ source('GDH_GOLD', 'dim_product_additional') }}
WHERE src_sys_nm = "US NABCA SAM"
AND is_active IS TRUE

GROUP BY 1
)hie
ON prd.product_key = hie.product_key

WHERE prd.src_sys_nm = "US NABCA SAM"
AND prd.is_active IS TRUE
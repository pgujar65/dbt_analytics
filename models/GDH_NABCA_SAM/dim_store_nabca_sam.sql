{{ config(schema=generate_schema_name('GDH_US_VIEW'),
             materialized='view',

            )}}

SELECT
st.store_key
,store_id
,store_name
,marketing_group
,hl_account_name
,custom_tdl_acct
,manager_name
,lifecycle
,tier
,terr_sgws
,terr_sgws_name
,store_lic_ind
,address_line1
,address_line2
,zip
,city
,state 
,county
,district
,license_num
,license_ind
,license_type

FROM {{ source('GDH_GOLD', 'dim_store') }} as st

LEFT JOIN
(
SELECT
store_key
,MAX(CASE WHEN attribute_master_id = "4101" THEN attribute_code END) AS manager_name
,MAX(CASE WHEN attribute_master_id = "4102" THEN attribute_code END) AS lifecycle
,MAX(CASE WHEN attribute_master_id = "4103" THEN attribute_code END) AS tier
,MAX(CASE WHEN attribute_master_id = "4104" THEN attribute_code END) AS terr_sgws
,MAX(CASE WHEN attribute_master_id = "4104" THEN attribute_desc END) AS terr_sgws_name
,MAX(CASE WHEN attribute_master_id = "4105" THEN attribute_code END) AS store_lic_ind
FROM {{ source('GDH_GOLD', 'dim_store_additional') }}
WHERE src_sys_nm = "US NABCA SAM"
AND is_active IS TRUE

GROUP BY 1
)hie
ON st.store_key = hie.store_key

LEFT JOIN 
{{ source('GDH_GOLD', 'dim_address') }} as adr
ON st.store_key = adr.address_key
AND adr.src_sys_nm = "US NABCA SAM"
AND adr.party_type_key = "3"
AND adr.is_active IS TRUE

LEFT JOIN 
{{ source('GDH_GOLD', 'dim_license') }} as lic
ON st.store_key = lic.applicant_key
AND lic.src_sys_nm = "US NABCA SAM"
AND lic.party_type_key = "3"
AND lic.is_active IS TRUE

WHERE st.src_sys_nm = "US NABCA SAM"
AND st.is_active IS TRUE
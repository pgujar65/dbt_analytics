{% snapshot dim_product %}

{{
    config(
      target_database='analytics-424809',
      target_schema='GDH_GOLD',
      unique_key='product_key',

      strategy='check',
      check_cols=['beam_product_key','short_product_desc','long_product_desc','alcohol_type','brand_extension','brand_category','brand_subcategory','segment','brand_family','age','flavor','container_type','bottle_per_case','proof','alcohol_content','size','size_in_ml','upc','manufacturer','src_sys_nm'],  
      updated_at='rec_crt_dt',
    )
}}

SELECT *,
true as is_active
 FROM {{ source('GDH_SILVER', 'dim_product') }}
WHERE rec_crt_dt IS NOT NULL

{% endsnapshot %}

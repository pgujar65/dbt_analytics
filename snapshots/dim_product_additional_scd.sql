{% snapshot dim_product_additional %}

{{
    config(
      target_database='analytics-424809',
      target_schema='GDH_GOLD',
      unique_key='composite_key',
      strategy='check',
      check_cols=['attribute_desc', 'src_sys_nm'],  
      updated_at='rec_crt_dt',
    )
}}

SELECT
  *,
  CONCAT(product_key, "-", attribute_master_id, "-", attribute_code) AS composite_key,
  true as is_active
FROM {{ source('GDH_SILVER', 'dim_product_additional') }}
WHERE rec_crt_dt IS NOT NULL

{% endsnapshot %}

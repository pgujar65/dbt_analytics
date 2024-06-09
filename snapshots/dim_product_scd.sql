{% snapshot dim_product %}

{{
    config(
      target_database='analytics-424809',
      target_schema='GDH_GOLD',
      unique_key='product_key',

      strategy='check',
      check_cols=['short_product_desc', 'long_product_desc', 'alcohol_type', 'rec_crt_dt'],  
      updated_at='rec_crt_dt',
    )
}}

select * from {{ source('GDH_SILVER', 'dim_product') }}

{% endsnapshot %}

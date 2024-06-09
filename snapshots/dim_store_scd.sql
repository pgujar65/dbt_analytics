{% snapshot dim_store %}

{{
    config(
      target_database='analytics-424809',
      target_schema='GDH_GOLD',
      unique_key='store_key',

      strategy='check',
      check_cols=['store_name'],  
      updated_at='rec_crt_dt',
    )
}}

select * from {{ source('GDH_SILVER', 'dim_store') }}

{% endsnapshot %}

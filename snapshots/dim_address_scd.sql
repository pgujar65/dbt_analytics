{% snapshot dim_address %}

{{
    config(
      target_database='analytics-424809',
      target_schema='GDH_GOLD',
      unique_key='address_key',

      strategy='check',
      check_cols=['address_key','party_type_key','address_line1','address_line2','address_line3','division','airport','terminal','city','district','county','state','country','sub_region','region','zip','area','latitude','longitude','src_sys_nm'],  
      updated_at='rec_crt_dt',
    )
}}

SELECT *,true as is_active
 FROM {{ source('GDH_SILVER', 'dim_address') }}
WHERE rec_crt_dt IS NOT NULL

{% endsnapshot %}
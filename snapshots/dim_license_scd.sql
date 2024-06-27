{% snapshot dim_license %}

{{
    config(
      target_database='analytics-424809',
      target_schema='GDH_GOLD',
      unique_key='applicant_key',

      strategy='check',
      check_cols=['party_type_key','applicant_id','license_num','license_ind','license_type','src_sys_nm'],  
      updated_at='rec_crt_dt',
    )
}}

SELECT *,
true as is_active
 FROM {{ source('GDH_SILVER', 'dim_license') }}
WHERE rec_crt_dt IS NOT NULL

{% endsnapshot %}
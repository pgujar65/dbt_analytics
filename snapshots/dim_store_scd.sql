{% snapshot dim_store %}

{{
    config(
      target_database='analytics-424809',
      target_schema='GDH_GOLD',
      unique_key='store_key',

      strategy='check',
      check_cols=['store_id','store_name','account_num','premise_type','premise_sub_type','trade_channel','trade_sub_channel','marketing_group','kpi_store','store_route','store_route_ly','store_route_group','store_route_group_ly','client_store_id','store_route_group_freeze','store_route_freeze','store_route_maturity','store_route_growth','store_hours','food_type','bcstvw_tier','bcstvw_tier_ly','bcstvw_tier_py','bcfglopw_indicator','chain_status','annual_volume','bottle_service','avg_weekly_volume','census_block_id','gsc_place_id','dma_name','ecommerce_pure_play','fulltime_equiv','checkout_count','place_name','square_footage','state_county_fips_code','ihub','ihub_high','ihub_high_py','ihub_high_ly','segmentation','segmentation_py','segmentation_ly','kpi_univ','kpi_univ_ly','kpi_univ_freeze','tdl_acct_group_type','ll_account_name','hl_account_name','cocktail_menu','bins_counter_units','cross_merchandising_allowed','rack_send_caps','samplings','pos','multicultural_focus','max_large_brand_display_potential','spirits_listing','quality','status','typical_display_size_all_other','typical_large_brand_display_size','well','window_display','window_door_signage','custom_tdl_acct','fb_trade_channel','floor_stacking','store_size','visit_days','delivery_days','order_days','online_order','traffic','src_sys_nm'],  
      updated_at='rec_crt_dt',
    )
}}

SELECT *,
true as is_active
 FROM  {{ source('GDH_SILVER', 'dim_store') }}
WHERE rec_crt_dt IS NOT NULL

{% endsnapshot %}

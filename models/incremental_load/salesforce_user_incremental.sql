{{ config(
    materialized='incremental',
    unique_key='id',
    schema=generate_schema_name('HARMONIZATION')
) }}

with source as (
    select
        id,
        username,
        lastname,
        firstname,
        name,
        companyname,
        division,
        department,
        title,
        street,
        city,
        state,
        last_modified_date,
    from {{ ref('raw_in_salesforce_user') }}
    {% if is_incremental() %}
        where last_modified_date > (select max(last_modified_date) from {{ this }})
    {% endif %}
),

final as (
    select
        id,
        username,
        lastname,
        firstname,
        name,
        companyname,
        division,
        department,
        title,
        street,
        city,
        state,
        last_modified_date,
    from source
)

select * from final

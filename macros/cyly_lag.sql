-- File: macros/get_last_year_value.sql
-- Need to create dummy rows for all the dates with dimention combinations before using this function
{% macro get_last_year_value(date_column, value_column) %}
    lag({{ value_column }}, 12) over (partition by product_key,store_key order by {{ date_column }})
{% endmacro %}

-- File: macros/get_last_year_value.sql
{% macro get_last_year_value_jon(date_column, value_column) %}
    (
        select
            {{ value_column }}
        from {{ this }}
        where
            id = outer.id
            and date_trunc('month', {{ date_column }}) = date_trunc('month', dateadd(year, -1, outer.{{ date_column }}))
    ) as value_ly
{% endmacro %}

{% macro rolling_sum(column, date_column, period) %}
    sum({{ column }}) over (
        partition by product_key,store_key
        order by {{ date_column }}
        rows between {{ period }} preceding and current row
    )
{% endmacro %}

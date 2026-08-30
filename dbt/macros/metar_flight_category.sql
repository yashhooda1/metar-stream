{% macro metar_flight_category(ceiling_col, visibility_col) %}
-- FAA flight category. Ceiling is the lowest BKN/OVC layer in feet AGL;
-- a null ceiling means unlimited, which never constrains the category.
case
    when {{ visibility_col }} is null and {{ ceiling_col }} is null then 'UNKNOWN'
    when coalesce({{ ceiling_col }}, 99999) <  500 or coalesce({{ visibility_col }}, 99) <  1 then 'LIFR'
    when coalesce({{ ceiling_col }}, 99999) < 1000 or coalesce({{ visibility_col }}, 99) <  3 then 'IFR'
    when coalesce({{ ceiling_col }}, 99999) <= 3000 or coalesce({{ visibility_col }}, 99) <= 5 then 'MVFR'
    else 'VFR'
end
{% endmacro %}


{% macro metar_incremental_filter(timestamp_column) %}
{#- Reprocess a trailing window so late-arriving corrections are picked up. -#}
{% if is_incremental() %}
    and {{ timestamp_column }} >= (
        select coalesce(max({{ timestamp_column }}), timestamp('1970-01-01'))
               - interval {{ var('metar_lookback_hours') }} hours
        from {{ this }}
    )
{% endif %}
{% endmacro %}

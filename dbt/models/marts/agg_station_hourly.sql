{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'station_hour_key',
    on_schema_change = 'sync_all_columns',
    partition_by = ['obs_date'],
    file_format = 'delta'
  )
}}

with observations as (

    select * from {{ ref('fct_observations') }}
    where 1 = 1
    {% if is_incremental() %}
        and observed_hour >= (
            select coalesce(max(observed_hour), timestamp('1970-01-01'))
                   - interval {{ var('metar_lookback_hours') }} hours
            from {{ this }}
        )
    {% endif %}

)

select
    {{ dbt_utils.generate_surrogate_key(['station_id', 'observed_hour']) }} as station_hour_key,
    station_id,
    observed_hour,
    to_date(observed_hour)                    as obs_date,

    count(*)                                  as observation_count,
    sum(case when temp_c is null then 1 else 0 end) as null_temp_count,
    avg(temp_c)                               as avg_temp_c,
    min(temp_c)                               as min_temp_c,
    max(temp_c)                               as max_temp_c,
    avg(relative_humidity_pct)                as avg_relative_humidity_pct,
    avg(wind_speed_kt)                        as avg_wind_speed_kt,
    max(peak_wind_kt)                         as peak_gust_kt,
    min(visibility_mi)                        as min_visibility_mi,
    min(ceiling_ft_agl)                       as min_ceiling_ft_agl,
    avg(altimeter_hpa)                        as avg_altimeter_hpa,
    avg(lag_seconds)                          as avg_ingest_lag_s,

    -- Worst category in the hour is the operationally meaningful one
    min(case flight_category
            when 'LIFR' then 1 when 'IFR' then 2
            when 'MVFR' then 3 when 'VFR' then 4 else 9
        end)                                  as worst_category_rank,
    sum(case when flight_category in ('IFR', 'LIFR') then 1 else 0 end) as ifr_observation_count,

    current_timestamp()                       as _dbt_loaded_at

from observations
group by station_id, observed_hour

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'observation_key',
    partition_by = ['obs_date'],
    file_format = 'delta'
  )
}}

with observations as (

    select * from {{ ref('stg_metar__observations') }}
    where 1 = 1
    {{ metar_incremental_filter('observed_at') }}

),

ceiling as (

    select * from {{ ref('int_metar__ceiling') }}

),

joined as (

    select
        o.observation_key,
        o.station_id,
        o.station_name,
        o.observed_at,
        o.obs_date,
        date_trunc('hour', o.observed_at)      as observed_hour,
        o.ingested_at,
        o.lag_seconds,

        o.temp_c,
        o.dewpoint_c,
        -- Magnus approximation, accurate to well under 1% at surface conditions
        case
            when o.temp_c is not null and o.dewpoint_c is not null
            -- METAR reports whole degrees, so rounding can put dewpoint just
            -- above temperature. Clamp at saturation; dewpoint_spread_c keeps
            -- the anomaly visible.
            then least(100, 100 * exp((17.625 * o.dewpoint_c) / (243.04 + o.dewpoint_c)
                                    - (17.625 * o.temp_c)     / (243.04 + o.temp_c)))
        end                                    as relative_humidity_pct,
        o.temp_c - o.dewpoint_c                as dewpoint_spread_c,

        o.wind_dir_deg,
        o.wind_speed_kt,
        o.wind_gust_kt,
        coalesce(o.wind_gust_kt, o.wind_speed_kt) as peak_wind_kt,
        coalesce(o.wind_gust_kt, o.wind_speed_kt) - o.wind_speed_kt as gust_factor_kt,

        o.visibility_mi,
        o.visibility_raw,
        c.ceiling_ft_agl,
        c.lowest_layer_ft_agl,
        c.layer_count,
        c.is_clear,
        c.has_parse_conflict,

        o.altimeter_hpa,
        o.altimeter_in_hg,
        o.raw_text,

        o.flight_category_noaa,
        {{ metar_flight_category('c.ceiling_ft_agl', 'o.visibility_mi') }} as flight_category_derived,

        current_timestamp()                    as _dbt_loaded_at

    from observations o
    left join ceiling c
        on o.observation_key = c.observation_key

)

select
    *,
    -- NOAA's category is authoritative when present; the derived one fills gaps
    coalesce(flight_category_noaa, flight_category_derived) as flight_category,
    case
        when flight_category_noaa is null then null
        else flight_category_noaa = flight_category_derived
    end as category_agrees
from joined

{{
  config(
    materialized = 'view'
  )
}}

-- Spark already applies dropDuplicates(["station_id", "observed_at"]) under a
-- 30-minute watermark, so no dedup window is needed here. This model types,
-- renames, and derives the unit conversions Silver does not carry.

with source as (

    select * from {{ source('metar_silver', 'observations') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['station_id', 'observed_at']) }} as observation_key,

    upper(trim(station_id))                as station_id,
    name                                   as station_name,
    observed_at,
    ingested_at,
    obs_date,
    lag_seconds,

    lat                                    as latitude,
    lon                                    as longitude,
    elevation_m,
    elevation_m * 3.28084                  as elevation_ft,

    temp_c,
    dewpoint_c,
    wind_dir_deg,
    wind_speed_kt,
    wind_gust_kt,

    visibility_mi,
    visibility_sm                          as visibility_raw,

    altimeter_hpa,
    altimeter_hpa / 33.8639                as altimeter_in_hg,

    upper(nullif(trim(flight_category), '')) as flight_category_noaa,
    raw_text

from source
where station_id is not null
  and observed_at is not null

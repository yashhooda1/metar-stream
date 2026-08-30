{{
  config(
    materialized = 'table',
    file_format = 'delta'
  )
}}

-- Silver carries station attributes on every observation row rather than in a
-- separate table, so the dimension is built from observed reality: only
-- stations that have actually reported appear here.

with observations as (

    select * from {{ ref('fct_observations') }}

),

latest_attributes as (

    select
        station_id,
        station_name,
        latitude,
        longitude,
        elevation_m,
        elevation_ft,
        row_number() over (
            partition by station_id order by observed_at desc
        ) as rn
    from {{ ref('stg_metar__observations') }}
    where latitude is not null and longitude is not null

),

activity as (

    select
        station_id,
        min(observed_at)                as first_observed_at,
        max(observed_at)                as last_observed_at,
        count(*)                        as observation_count,
        avg(lag_seconds)                as avg_lag_seconds
    from observations
    group by station_id

)

select
    a.station_id,
    l.station_name,
    l.latitude,
    l.longitude,
    l.elevation_m,
    l.elevation_ft,
    a.first_observed_at,
    a.last_observed_at,
    a.observation_count,
    a.avg_lag_seconds,
    -- Most stations report on a 20-60 minute cadence; three hours of silence
    -- means the station is down, not merely between reports
    a.last_observed_at >= current_timestamp() - interval 3 hours as is_reporting
from activity a
left join latest_attributes l
    on a.station_id = l.station_id
   and l.rn = 1

{{
  config(
    materialized = 'view'
  )
}}

-- Parity between the PySpark Gold table and the dbt-built aggregate.
-- gold/metar_15min is windowed at 15 minutes; agg_station_hourly at 60. The
-- Spark side is rolled up to hourly first so the two are comparable.
--
-- Note the weighted average: avg(avg_temp_c) across four windows is only
-- correct when each window holds the same number of observations, which for
-- stations reporting on a 20-60 minute cadence is almost never true.

with spark_15min as (

    select * from {{ source('metar_gold_spark', 'metar_15min') }}

),

spark_hourly as (

    select
        station_id,
        date_trunc('hour', window_start)                                as observed_hour,
        sum(avg_temp_c * observation_count) / nullif(sum(observation_count), 0) as avg_temp_c,
        max(peak_gust_kt)                                               as peak_gust_kt,
        min(min_visibility_mi)                                          as min_visibility_mi,
        sum(observation_count)                                          as observation_count
    from spark_15min
    group by station_id, date_trunc('hour', window_start)

),

dbt_hourly as (

    select
        station_id,
        observed_hour,
        avg_temp_c,
        peak_gust_kt,
        min_visibility_mi,
        observation_count,
        null_temp_count
    from {{ ref('agg_station_hourly') }}

),

compared as (

    select
        coalesce(s.station_id, d.station_id)       as station_id,
        coalesce(s.observed_hour, d.observed_hour) as observed_hour,

        s.observation_count                        as spark_observation_count,
        d.observation_count                        as dbt_observation_count,
        s.avg_temp_c                               as spark_avg_temp_c,
        d.avg_temp_c                               as dbt_avg_temp_c,
        s.peak_gust_kt                             as spark_peak_gust_kt,
        d.peak_gust_kt                             as dbt_peak_gust_kt,
        s.min_visibility_mi                        as spark_min_visibility_mi,
        d.min_visibility_mi                        as dbt_min_visibility_mi,

        case
            when s.station_id is null then 'MISSING_IN_SPARK'
            when d.station_id is null then 'MISSING_IN_DBT'
            when s.observation_count <> d.observation_count then 'COUNT_MISMATCH'
            -- Spark's avg ignores null temps but its observation_count does not,
            -- so weighting by that count is only valid when no temps are null
            when d.null_temp_count = 0
                 and abs(coalesce(s.avg_temp_c, 0) - coalesce(d.avg_temp_c, 0)) > 0.05 then 'TEMP_MISMATCH'
            when not (s.peak_gust_kt <=> d.peak_gust_kt) then 'GUST_MISMATCH'
            when abs(coalesce(s.min_visibility_mi, 0) - coalesce(d.min_visibility_mi, 0)) > 0.01 then 'VISIBILITY_MISMATCH'
            else 'MATCH'
        end as diff_type

    from spark_hourly s
    full outer join dbt_hourly d
        on s.station_id = d.station_id
       and s.observed_hour = d.observed_hour

)

select * from compared
where diff_type <> 'MATCH'
  -- Spark emits a window only once the watermark passes its end, so the newest
  -- hour is always open on the streaming side. Bound the comparison by how far
  -- Spark has actually closed, not by wall clock -- a stopped or lagging stream
  -- would otherwise show every recent hour as missing.
  and observed_hour < (select max(observed_hour) from spark_hourly)

-- A METAR timestamped ahead of now means a clock or parse problem upstream.
-- Small tolerance for stations reporting slightly ahead of the hour.
select
    observation_key,
    station_id,
    observed_at
from {{ ref('fct_observations') }}
where observed_at > current_timestamp() + interval 15 minutes

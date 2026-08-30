-- Guards the macro against the data: a derived VFR row can never carry a
-- ceiling at or below 3000 ft or visibility at or below 5 sm, by definition.
-- This tests the derivation, not NOAA's published value.
select
    observation_key,
    station_id,
    observed_at,
    ceiling_ft_agl,
    visibility_mi,
    flight_category_derived
from {{ ref('fct_observations') }}
where flight_category_derived = 'VFR'
  and (ceiling_ft_agl <= 3000 or visibility_mi <= 5)

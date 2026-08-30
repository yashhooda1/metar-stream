{{
  config(
    materialized = 'view'
  )
}}

-- Grades the ceiling parser against NOAA. Every row where the category derived
-- from raw_text disagrees with the published flight_category is either a parse
-- bug or a genuine NOAA edge case. Both are worth seeing.
--
-- Expected residue: observations with BKN/// or VV/// (unknown height), where
-- no ceiling can be parsed and the derived category will read too optimistic.

with observations as (

    select * from {{ ref('fct_observations') }}
    where flight_category_noaa is not null

),

graded as (

    select
        flight_category_noaa,
        flight_category_derived,
        count(*)                                        as observation_count,
        sum(case when has_parse_conflict then 1 else 0 end) as parse_conflict_count,
        sum(case when ceiling_ft_agl is null then 1 else 0 end) as null_ceiling_count,
        sum(case when raw_text rlike '(BKN|OVC|VV)///' then 1 else 0 end) as unknown_height_count
    from observations
    group by flight_category_noaa, flight_category_derived

)

select
    *,
    flight_category_noaa = flight_category_derived as is_agreement,
    round(100.0 * observation_count
          / sum(observation_count) over (), 2)    as pct_of_total
from graded
order by is_agreement, observation_count desc

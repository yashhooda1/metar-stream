{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'transition_key',
    partition_by = ['obs_date'],
    file_format = 'delta'
  )
}}

-- One row per change in flight category at a station: when conditions
-- deteriorated or improved, how long the previous state held, and by how
-- many steps it moved. This is the model the dashboard alerts off.

with observations as (

    select
        observation_key,
        station_id,
        observed_at,
        flight_category,
        visibility_mi,
        ceiling_ft_agl
    from {{ ref('fct_observations') }}
    where flight_category not in ('UNKNOWN')
    {% if is_incremental() %}
        -- Wider lookback than the fact table: a transition needs the prior row,
        -- which may sit outside the incremental window.
        and observed_at >= (
            select coalesce(max(observed_at), timestamp('1970-01-01'))
                   - interval {{ var('metar_lookback_hours') * 4 }} hours
            from {{ this }}
        )
    {% endif %}

),

sequenced as (

    select
        *,
        lag(flight_category) over (
            partition by station_id order by observed_at
        ) as previous_category,
        lag(observed_at) over (
            partition by station_id order by observed_at
        ) as previous_observed_at
    from observations

),

transitions as (

    select
        {{ dbt_utils.generate_surrogate_key(['station_id', 'observed_at']) }} as transition_key,
        station_id,
        observed_at,
        to_date(observed_at)        as obs_date,
        previous_category,
        flight_category             as new_category,
        previous_observed_at,
        (unix_timestamp(observed_at) - unix_timestamp(previous_observed_at)) / 60.0
                                    as previous_state_minutes,
        visibility_mi,
        ceiling_ft_agl,

        case flight_category
            when 'LIFR' then 1 when 'IFR' then 2
            when 'MVFR' then 3 when 'VFR' then 4
        end
        -
        case previous_category
            when 'LIFR' then 1 when 'IFR' then 2
            when 'MVFR' then 3 when 'VFR' then 4
        end                         as category_step_change,

        current_timestamp()         as _dbt_loaded_at

    from sequenced
    where previous_category is not null
      and previous_category <> flight_category

)

select
    *,
    case
        when category_step_change < 0 then 'DETERIORATION'
        else 'IMPROVEMENT'
    end as transition_direction
from transitions

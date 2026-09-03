{{
  config(
    materialized = 'view'
  )
}}

-- Silver carries no cloud structure, so the ceiling is parsed out of raw_text.
-- METAR cloud groups are three letters plus three digits of hundreds of feet:
-- "BKN008" is a broken layer at 800 ft AGL. Only BKN, OVC, and VV (indefinite
-- ceiling / vertical visibility) constitute a ceiling; FEW and SCT never do,
-- however low they sit.
--
-- Two things are deliberately excluded:
--   * everything after RMK, where cloud-like tokens appear that are not the
--     current observation
--   * groups with slashes (BKN///, VV///), where the height is unknown --
--     \d{3} simply fails to match, leaving a null rather than a fake zero

with observations as (

    select
        observation_key,
        station_id,
        observed_at,
        -- Trim remarks before parsing
        case
            when raw_text like '% RMK %' then split(raw_text, ' RMK ')[0]
            else raw_text
        end as body

    from {{ ref('stg_metar__observations') }}

),

parsed as (

    select
        observation_key,
        station_id,
        observed_at,

        transform(
            regexp_extract_all(body, '(BKN|OVC|VV)([0-9]{3})', 2),
            x -> cast(x as int) * 100
        ) as ceiling_layers_ft,

        transform(
            regexp_extract_all(body, '(FEW|SCT|BKN|OVC|VV)([0-9]{3})', 2),
            x -> cast(x as int) * 100
        ) as all_layers_ft,

        body rlike '\\b(CLR|SKC|NCD|NSC|CAVOK)\\b' as is_clear_reported

    from observations

)

select
    observation_key,
    station_id,
    observed_at,
    array_min(ceiling_layers_ft)              as ceiling_ft_agl,
    array_min(all_layers_ft)                  as lowest_layer_ft_agl,
    size(all_layers_ft)                       as layer_count,
    is_clear_reported                         as is_clear,
    -- A clear report and a parsed ceiling cannot both be true; surfaced so the
    -- parse can be audited rather than silently trusted
    is_clear_reported and array_min(ceiling_layers_ft) is not null as has_parse_conflict
from parsed

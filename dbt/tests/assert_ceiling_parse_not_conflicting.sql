{{ config(severity='warn', warn_if='>0', error_if='>50') }}

-- Canary for parser drift, not a correctness gate. Individual malformed
-- reports occur (KHST 2026-08-31 lists SCT024 BKN200 -DZ CLR in one
-- observation). Fails the build only if conflicts become systematic.

-- A clear-sky report and a parsed ceiling cannot both be true. More than a
-- handful means the raw_text regex is matching something it should not --
-- most likely cloud-like tokens that survived the RMK trim.
select
    observation_key,
    station_id,
    observed_at,
    raw_text,
    ceiling_ft_agl
from {{ ref('fct_observations') }}
where has_parse_conflict

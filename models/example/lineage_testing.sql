
-- Target table: REPORTING.lineage_testing
INSERT INTO  {{ source('REPORTING', 'lineage_testing') }}

SELECT DISTINCT sr.region, sr.state
FROM {{ source('HARMONIZATION', 'sr_detail_asset_join') }} sr



  create or replace   view ATMOSYNC_DB.ANALYTICS.stg_container_telemetry
  
  
  
  
  as (
    WITH raw_source AS (
    SELECT * FROM ATMOSYNC_DB.RAW.RAW_CONTAINER_TELEMETRY
)

SELECT
    raw_payload:event_id::STRING AS event_id,
    raw_payload:container_id::STRING AS container_id,
    raw_payload:commodity::STRING AS commodity,
    raw_payload:timestamp::TIMESTAMP AS event_timestamp,
    raw_payload:telemetry.temperature_c::FLOAT AS temperature_c,
    raw_payload:telemetry.humidity_pct::FLOAT AS humidity_pct,
    raw_payload:telemetry.vibration_g::FLOAT AS vibration_g,
    raw_payload:location.latitude::FLOAT AS latitude,
    raw_payload:location.longitude::FLOAT AS longitude,
    ingested_at
FROM raw_source
  );


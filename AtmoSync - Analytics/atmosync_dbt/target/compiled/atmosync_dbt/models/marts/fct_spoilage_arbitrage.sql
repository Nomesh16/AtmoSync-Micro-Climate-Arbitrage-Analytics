WITH latest_telemetry AS (
    SELECT 
        container_id,
        commodity,
        temperature_c,
        humidity_pct,
        latitude,
        longitude,
        event_timestamp,
        ROW_NUMBER() OVER (PARTITION BY container_id ORDER BY event_timestamp DESC) as rn
    FROM ATMOSYNC_DB.ANALYTICS.stg_container_telemetry
),

current_status AS (
    SELECT * 
    FROM latest_telemetry 
    WHERE rn = 1
),

calculated_risk AS (
    SELECT 
        t.container_id,
        t.commodity,
        t.temperature_c,
        t.latitude AS current_lat,
        t.longitude AS current_lon,
        t.event_timestamp,
        
        -- Quality degradation: 12.5% loss per degree above 6.0°C threshold
        CASE 
            WHEN t.temperature_c > 6.0 THEN GREATEST(0, (t.temperature_c - 6.0) * 12.5)
            ELSE 0 
        END AS quality_degradation_pct,
        
        -- Thermal risk classification
        CASE 
            WHEN t.temperature_c > 6.0 THEN 'CRITICAL - HIGH SPOILAGE'
            WHEN t.temperature_c > 4.5 THEN 'WARNING - TEMP DRIFT'
            ELSE 'OPTIMAL'
        END AS thermal_status
    FROM current_status t
)

SELECT 
    cr.container_id,
    cr.commodity,
    cr.temperature_c,
    cr.current_lat,
    cr.current_lon,
    cr.event_timestamp,
    cr.quality_degradation_pct,
    cr.thermal_status,
    mp.market_id AS recommended_reroute_market,
    mp.market_name,
    mp.base_price_per_ton,
    
    -- Calculated Salvage Value (Remaining market value per ton)
    ROUND(mp.base_price_per_ton * (1 - (cr.quality_degradation_pct / 100.0)), 2) AS salvage_value_per_ton,
    
    -- Estimated Financial Loss Prevented per ton by rerouting
    ROUND(mp.base_price_per_ton * (cr.quality_degradation_pct / 100.0), 2) AS arbitrage_loss_prevented
FROM calculated_risk cr
CROSS JOIN ATMOSYNC_DB.RAW.MARKET_PRICING mp
WHERE mp.commodity = cr.commodity
  AND mp.market_id = CASE 
                        WHEN cr.thermal_status = 'CRITICAL - HIGH SPOILAGE' THEN 'MKT-LIS' 
                        ELSE 'MKT-ROT' 
                     END
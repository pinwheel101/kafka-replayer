-- Trace table test data initialization script
-- Simulates semiconductor manufacturing trace data

-- Create trace table (simulates Hive table)
CREATE TABLE IF NOT EXISTS trace (
    ts TIMESTAMP,
    eqp_id VARCHAR(100),
    module_name VARCHAR(100),
    lot_cd VARCHAR(100),
    lot_id VARCHAR(100),
    slot_id VARCHAR(50),
    product_id VARCHAR(100),
    oper_id VARCHAR(50),
    oper_desc VARCHAR(200),
    recipe_id VARCHAR(100),
    ppid VARCHAR(100),
    step_id VARCHAR(50),
    step_name VARCHAR(200),
    param_name TEXT[],      -- Array of parameter names
    param_alias TEXT[],     -- Array of parameter aliases
    param_svid TEXT[],      -- Array of parameter SVIDs
    param_value TEXT[],     -- Array of parameter values
    batch_id VARCHAR(100),
    batch_type VARCHAR(50),
    zone VARCHAR(50),
    start_dtts TIMESTAMP,
    create_dtts TIMESTAMP,
    org_wf_id VARCHAR(100),
    alias_lot_id VARCHAR(100),
    fdc_lot_cd VARCHAR(100),
    target TEXT[],          -- Array of target values
    lsl TEXT[],             -- Array of lower spec limit
    lcl TEXT[],             -- Array of lower control limit
    ucl TEXT[],             -- Array of upper control limit
    usl TEXT[],             -- Array of upper spec limit
    spec_model_name TEXT[], -- Array of spec model names
    dt VARCHAR(10),         -- Partition column (date)
    fab VARCHAR(50)         -- Partition column (fab)
);

-- Create indexes for faster queries
CREATE INDEX idx_trace_dt ON trace(dt);
CREATE INDEX idx_trace_lot_id ON trace(lot_id);
CREATE INDEX idx_trace_ts ON trace(ts);

-- Insert sample trace data (50 events across 5 lots)
INSERT INTO trace (
    ts, eqp_id, module_name, lot_cd, lot_id, slot_id, product_id,
    oper_id, oper_desc, recipe_id, ppid, step_id, step_name,
    param_name, param_alias, param_svid, param_value,
    batch_id, batch_type, zone, start_dtts, create_dtts,
    org_wf_id, alias_lot_id, fdc_lot_cd,
    target, lsl, lcl, ucl, usl, spec_model_name,
    dt, fab
)
SELECT
    -- Timestamps (spread over 1 hour with 1 minute intervals)
    NOW() + (s * INTERVAL '1 minute') AS ts,

    -- Equipment and module
    'EQP_' || ((s % 5) + 1) AS eqp_id,
    'MODULE_' || CHAR(65 + (s % 3)) AS module_name,

    -- Lot information
    'LOT' || LPAD(((s / 10) + 1)::TEXT, 6, '0') AS lot_cd,
    'L' || LPAD(((s / 10) + 1)::TEXT, 8, '0') AS lot_id,
    'SLOT_' || LPAD((s % 10 + 1)::TEXT, 2, '0') AS slot_id,
    'PROD_' || CHAR(65 + (s % 5)) AS product_id,

    -- Operation info
    'OPER_' || LPAD(((s % 20) + 1)::TEXT, 3, '0') AS oper_id,
    'Operation ' || ((s % 20) + 1) || ' Description' AS oper_desc,
    'RECIPE_' || LPAD(((s % 10) + 1)::TEXT, 3, '0') AS recipe_id,
    'PPID_' || LPAD(((s % 15) + 1)::TEXT, 4, '0') AS ppid,
    'STEP_' || LPAD(((s % 8) + 1)::TEXT, 2, '0') AS step_id,
    'Step ' || ((s % 8) + 1) || ' Process' AS step_name,

    -- Parameter arrays (3-5 parameters per event)
    ARRAY['TEMP', 'PRESSURE', 'FLOW', 'TIME', 'POWER'][1:(3 + (s % 3))] AS param_name,
    ARRAY['Temperature', 'Pressure', 'Flow Rate', 'Process Time', 'RF Power'][1:(3 + (s % 3))] AS param_alias,
    ARRAY['SVID_001', 'SVID_002', 'SVID_003', 'SVID_004', 'SVID_005'][1:(3 + (s % 3))] AS param_svid,
    ARRAY[
        (200 + (random() * 100))::TEXT,
        (500 + (random() * 200))::TEXT,
        (100 + (random() * 50))::TEXT,
        (30 + (random() * 20))::TEXT,
        (1000 + (random() * 500))::TEXT
    ][1:(3 + (s % 3))] AS param_value,

    -- Batch info
    'BATCH_' || LPAD(((s / 5) + 1)::TEXT, 6, '0') AS batch_id,
    CASE (s % 3)
        WHEN 0 THEN 'NORMAL'
        WHEN 1 THEN 'HOT'
        ELSE 'MONITOR'
    END AS batch_type,
    'ZONE_' || CHAR(65 + (s % 4)) AS zone,

    -- Timing
    NOW() + (s * INTERVAL '1 minute') - INTERVAL '5 minutes' AS start_dtts,
    NOW() + (s * INTERVAL '1 minute') AS create_dtts,

    -- IDs
    'WF_' || LPAD(((s / 10) + 1)::TEXT, 6, '0') AS org_wf_id,
    'ALIAS_L' || LPAD(((s / 10) + 1)::TEXT, 8, '0') AS alias_lot_id,
    'FDC_LOT' || LPAD(((s / 10) + 1)::TEXT, 6, '0') AS fdc_lot_cd,

    -- Spec arrays (matching param arrays)
    ARRAY[
        (250 + (random() * 20))::TEXT,
        (600 + (random() * 50))::TEXT,
        (125 + (random() * 10))::TEXT,
        (40 + (random() * 5))::TEXT,
        (1200 + (random() * 100))::TEXT
    ][1:(3 + (s % 3))] AS target,

    ARRAY[
        (150 + (random() * 20))::TEXT,
        (400 + (random() * 50))::TEXT,
        (80 + (random() * 10))::TEXT,
        (20 + (random() * 5))::TEXT,
        (800 + (random() * 100))::TEXT
    ][1:(3 + (s % 3))] AS lsl,

    ARRAY[
        (180 + (random() * 20))::TEXT,
        (450 + (random() * 50))::TEXT,
        (90 + (random() * 10))::TEXT,
        (25 + (random() * 5))::TEXT,
        (900 + (random() * 100))::TEXT
    ][1:(3 + (s % 3))] AS lcl,

    ARRAY[
        (320 + (random() * 20))::TEXT,
        (750 + (random() * 50))::TEXT,
        (160 + (random() * 10))::TEXT,
        (55 + (random() * 5))::TEXT,
        (1500 + (random() * 100))::TEXT
    ][1:(3 + (s % 3))] AS ucl,

    ARRAY[
        (350 + (random() * 20))::TEXT,
        (800 + (random() * 50))::TEXT,
        (170 + (random() * 10))::TEXT,
        (60 + (random() * 5))::TEXT,
        (1600 + (random() * 100))::TEXT
    ][1:(3 + (s % 3))] AS usl,

    ARRAY['SPEC_MODEL_' || CHAR(65 + ((s % 3)))][1:(3 + (s % 3))] AS spec_model_name,

    -- Partition columns
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS dt,
    'FAB_' || CHAR(65 + (s % 2)) AS fab

FROM generate_series(0, 49) AS s;

-- Insert data for yesterday (different date partition)
INSERT INTO trace (
    ts, eqp_id, module_name, lot_cd, lot_id, slot_id, product_id,
    oper_id, oper_desc, recipe_id, ppid, step_id, step_name,
    param_name, param_alias, param_svid, param_value,
    batch_id, batch_type, zone, start_dtts, create_dtts,
    org_wf_id, alias_lot_id, fdc_lot_cd,
    target, lsl, lcl, ucl, usl, spec_model_name,
    dt, fab
)
SELECT
    NOW() - INTERVAL '1 day' + (s * INTERVAL '1 minute') AS ts,
    'EQP_' || ((s % 5) + 1) AS eqp_id,
    'MODULE_' || CHAR(65 + (s % 3)) AS module_name,
    'LOT' || LPAD(((s / 10) + 100)::TEXT, 6, '0') AS lot_cd,
    'L' || LPAD(((s / 10) + 100)::TEXT, 8, '0') AS lot_id,
    'SLOT_' || LPAD((s % 10 + 1)::TEXT, 2, '0') AS slot_id,
    'PROD_' || CHAR(65 + (s % 5)) AS product_id,
    'OPER_' || LPAD(((s % 20) + 1)::TEXT, 3, '0') AS oper_id,
    'Operation ' || ((s % 20) + 1) || ' Description' AS oper_desc,
    'RECIPE_' || LPAD(((s % 10) + 1)::TEXT, 3, '0') AS recipe_id,
    'PPID_' || LPAD(((s % 15) + 1)::TEXT, 4, '0') AS ppid,
    'STEP_' || LPAD(((s % 8) + 1)::TEXT, 2, '0') AS step_id,
    'Step ' || ((s % 8) + 1) || ' Process' AS step_name,
    ARRAY['TEMP', 'PRESSURE', 'FLOW', 'TIME', 'POWER'][1:(3 + (s % 3))] AS param_name,
    ARRAY['Temperature', 'Pressure', 'Flow Rate', 'Process Time', 'RF Power'][1:(3 + (s % 3))] AS param_alias,
    ARRAY['SVID_001', 'SVID_002', 'SVID_003', 'SVID_004', 'SVID_005'][1:(3 + (s % 3))] AS param_svid,
    ARRAY[
        (200 + (random() * 100))::TEXT,
        (500 + (random() * 200))::TEXT,
        (100 + (random() * 50))::TEXT,
        (30 + (random() * 20))::TEXT,
        (1000 + (random() * 500))::TEXT
    ][1:(3 + (s % 3))] AS param_value,
    'BATCH_' || LPAD(((s / 5) + 100)::TEXT, 6, '0') AS batch_id,
    CASE (s % 3)
        WHEN 0 THEN 'NORMAL'
        WHEN 1 THEN 'HOT'
        ELSE 'MONITOR'
    END AS batch_type,
    'ZONE_' || CHAR(65 + (s % 4)) AS zone,
    NOW() - INTERVAL '1 day' + (s * INTERVAL '1 minute') - INTERVAL '5 minutes' AS start_dtts,
    NOW() - INTERVAL '1 day' + (s * INTERVAL '1 minute') AS create_dtts,
    'WF_' || LPAD(((s / 10) + 100)::TEXT, 6, '0') AS org_wf_id,
    'ALIAS_L' || LPAD(((s / 10) + 100)::TEXT, 8, '0') AS alias_lot_id,
    'FDC_LOT' || LPAD(((s / 10) + 100)::TEXT, 6, '0') AS fdc_lot_cd,
    ARRAY[
        (250 + (random() * 20))::TEXT,
        (600 + (random() * 50))::TEXT,
        (125 + (random() * 10))::TEXT,
        (40 + (random() * 5))::TEXT,
        (1200 + (random() * 100))::TEXT
    ][1:(3 + (s % 3))] AS target,
    ARRAY[
        (150 + (random() * 20))::TEXT,
        (400 + (random() * 50))::TEXT,
        (80 + (random() * 10))::TEXT,
        (20 + (random() * 5))::TEXT,
        (800 + (random() * 100))::TEXT
    ][1:(3 + (s % 3))] AS lsl,
    ARRAY[
        (180 + (random() * 20))::TEXT,
        (450 + (random() * 50))::TEXT,
        (90 + (random() * 10))::TEXT,
        (25 + (random() * 5))::TEXT,
        (900 + (random() * 100))::TEXT
    ][1:(3 + (s % 3))] AS lcl,
    ARRAY[
        (320 + (random() * 20))::TEXT,
        (750 + (random() * 50))::TEXT,
        (160 + (random() * 10))::TEXT,
        (55 + (random() * 5))::TEXT,
        (1500 + (random() * 100))::TEXT
    ][1:(3 + (s % 3))] AS ucl,
    ARRAY[
        (350 + (random() * 20))::TEXT,
        (800 + (random() * 50))::TEXT,
        (170 + (random() * 10))::TEXT,
        (60 + (random() * 5))::TEXT,
        (1600 + (random() * 100))::TEXT
    ][1:(3 + (s % 3))] AS usl,
    ARRAY['SPEC_MODEL_' || CHAR(65 + ((s % 3)))][1:(3 + (s % 3))] AS spec_model_name,
    TO_CHAR(CURRENT_DATE - INTERVAL '1 day', 'YYYY-MM-DD') AS dt,
    'FAB_' || CHAR(65 + (s % 2)) AS fab
FROM generate_series(0, 49) AS s;

-- Show summary
DO $$
DECLARE
    total_count INTEGER;
    date_count INTEGER;
    lot_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_count FROM trace;
    SELECT COUNT(DISTINCT dt) INTO date_count FROM trace;
    SELECT COUNT(DISTINCT lot_id) INTO lot_count FROM trace;

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Trace test data initialized successfully!';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Total events: %', total_count;
    RAISE NOTICE 'Distinct dates: %', date_count;
    RAISE NOTICE 'Distinct lots: %', lot_count;
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Sample query:';
    RAISE NOTICE '  SELECT lot_id, eqp_id, COUNT(*) FROM trace GROUP BY lot_id, eqp_id;';
    RAISE NOTICE '========================================';
END $$;

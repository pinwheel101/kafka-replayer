-- Test data initialization script
-- Simulates Hive table structure for testing

-- Create test table (simulates Hive table)
CREATE TABLE IF NOT EXISTS events (
    event_key VARCHAR(100),
    event_time BIGINT,
    user_id VARCHAR(50),
    event_type VARCHAR(50),
    payload TEXT,
    dt VARCHAR(10)
);

-- Create index for faster queries
CREATE INDEX idx_events_dt ON events(dt);
CREATE INDEX idx_events_time ON events(event_time);

-- Insert sample test data (100 events)
INSERT INTO events (event_key, event_time, user_id, event_type, payload, dt)
SELECT
    'event_' || generate_series,
    extract(epoch from (NOW() + (generate_series || ' seconds')::interval)) * 1000,
    'user_' || (random() * 10)::int,
    CASE (random() * 3)::int
        WHEN 0 THEN 'click'
        WHEN 1 THEN 'view'
        WHEN 2 THEN 'purchase'
        ELSE 'other'
    END,
    '{"data": "sample_' || generate_series || '", "value": ' || (random() * 100)::int || '}',
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
FROM generate_series(1, 100);

-- Insert data for different date (yesterday)
INSERT INTO events (event_key, event_time, user_id, event_type, payload, dt)
SELECT
    'event_' || (generate_series + 1000),
    extract(epoch from (NOW() - INTERVAL '1 day' + (generate_series || ' seconds')::interval)) * 1000,
    'user_' || (random() * 10)::int,
    CASE (random() * 3)::int
        WHEN 0 THEN 'click'
        WHEN 1 THEN 'view'
        WHEN 2 THEN 'purchase'
        ELSE 'other'
    END,
    '{"data": "sample_' || (generate_series + 1000) || '", "value": ' || (random() * 100)::int || '}',
    TO_CHAR(CURRENT_DATE - INTERVAL '1 day', 'YYYY-MM-DD')
FROM generate_series(1, 100);

-- Show summary
DO $$
DECLARE
    total_count INTEGER;
    date_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_count FROM events;
    SELECT COUNT(DISTINCT dt) INTO date_count FROM events;

    RAISE NOTICE 'Test data initialized successfully!';
    RAISE NOTICE 'Total events: %', total_count;
    RAISE NOTICE 'Distinct dates: %', date_count;
END $$;

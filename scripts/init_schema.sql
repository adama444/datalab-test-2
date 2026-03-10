-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create the datalab schema
CREATE SCHEMA IF NOT EXISTS datalab;

-- Table for Analytical Data
CREATE TABLE IF NOT EXISTS datalab.public_service_requests (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(50),
    category VARCHAR(100),
    status VARCHAR(50),
    commune VARCHAR(100),
    neighborhood VARCHAR(100),
    request_timestamp TIMESTAMPTZ,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    geom GEOMETRY(Point, 4326),
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table for Logs/Audit
CREATE TABLE IF NOT EXISTS datalab.pipeline_logs (
    id SERIAL PRIMARY KEY,
    bucket TEXT NOT NULL,
    filename TEXT NOT NULL UNIQUE,
    status VARCHAR(20),
    read_rows INTEGER,
    rows_written INTEGER,
    bad_records INTEGER,
    error_message TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Function to automatically sync Geometry column
CREATE OR REPLACE FUNCTION datalab.update_geom_from_latlon()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger
CREATE TRIGGER trg_update_geom
BEFORE INSERT OR UPDATE ON datalab.public_service_requests
FOR EACH ROW EXECUTE FUNCTION datalab.update_geom_from_latlon();

--- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_request_timestamp ON datalab.public_service_requests(request_timestamp);
CREATE INDEX IF NOT EXISTS idx_status ON datalab.public_service_requests(status);
CREATE INDEX IF NOT EXISTS idx_category ON datalab.public_service_requests(category);
CREATE INDEX IF NOT EXISTS idx_geom ON datalab.public_service_requests USING GIST(geom);
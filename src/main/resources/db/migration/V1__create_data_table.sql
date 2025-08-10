-- Create the main data ingestion table
CREATE TABLE IF NOT EXISTS ingested_data (
    id BIGSERIAL PRIMARY KEY,
    external_id VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(50),
    department VARCHAR(100),
    status VARCHAR(50),
    external_created_at TIMESTAMPTZ NOT NULL,
    external_updated_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_ingested_data_external_id ON ingested_data(external_id);
CREATE INDEX IF NOT EXISTS idx_ingested_data_email ON ingested_data(email);
CREATE INDEX IF NOT EXISTS idx_ingested_data_department ON ingested_data(department);
CREATE INDEX IF NOT EXISTS idx_ingested_data_status ON ingested_data(status);
CREATE INDEX IF NOT EXISTS idx_ingested_data_external_updated_at ON ingested_data(external_updated_at);
CREATE INDEX IF NOT EXISTS idx_ingested_data_updated_at ON ingested_data(updated_at);

-- Add comments for documentation
COMMENT ON TABLE ingested_data IS 'Table storing data ingested from external API';
COMMENT ON COLUMN ingested_data.id IS 'Internal unique identifier';
COMMENT ON COLUMN ingested_data.external_id IS 'Unique identifier from external system';
COMMENT ON COLUMN ingested_data.name IS 'Full name of the person';
COMMENT ON COLUMN ingested_data.email IS 'Email address';
COMMENT ON COLUMN ingested_data.phone IS 'Phone number';
COMMENT ON COLUMN ingested_data.department IS 'Department or organizational unit';
COMMENT ON COLUMN ingested_data.status IS 'Current status (active, inactive, pending, etc.)';
COMMENT ON COLUMN ingested_data.external_created_at IS 'Creation timestamp from external system';
COMMENT ON COLUMN ingested_data.external_updated_at IS 'Last update timestamp from external system';
COMMENT ON COLUMN ingested_data.created_at IS 'Record creation timestamp in our system';
COMMENT ON COLUMN ingested_data.updated_at IS 'Record last update timestamp in our system';
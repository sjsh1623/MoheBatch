-- Create job execution state tracking table
CREATE TABLE IF NOT EXISTS job_execution_state (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL UNIQUE,
    last_processed_page INTEGER NOT NULL DEFAULT 0,
    last_processed_timestamp TIMESTAMPTZ,
    total_processed_records BIGINT NOT NULL DEFAULT 0,
    last_execution_status VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for job state queries
CREATE INDEX IF NOT EXISTS idx_job_execution_state_job_name ON job_execution_state(job_name);
CREATE INDEX IF NOT EXISTS idx_job_execution_state_updated_at ON job_execution_state(updated_at);
CREATE INDEX IF NOT EXISTS idx_job_execution_state_status ON job_execution_state(last_execution_status);

-- Add comments for documentation
COMMENT ON TABLE job_execution_state IS 'Table tracking batch job execution state for resumability';
COMMENT ON COLUMN job_execution_state.id IS 'Internal unique identifier';
COMMENT ON COLUMN job_execution_state.job_name IS 'Name of the batch job';
COMMENT ON COLUMN job_execution_state.last_processed_page IS 'Last successfully processed page number';
COMMENT ON COLUMN job_execution_state.last_processed_timestamp IS 'Timestamp of last successful processing';
COMMENT ON COLUMN job_execution_state.total_processed_records IS 'Total number of records processed by this job';
COMMENT ON COLUMN job_execution_state.last_execution_status IS 'Status of the last job execution';
COMMENT ON COLUMN job_execution_state.created_at IS 'Record creation timestamp';
COMMENT ON COLUMN job_execution_state.updated_at IS 'Record last update timestamp';
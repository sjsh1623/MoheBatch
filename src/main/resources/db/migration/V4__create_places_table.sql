CREATE TABLE IF NOT EXISTS places (
    id BIGSERIAL PRIMARY KEY,
    locat_name VARCHAR(255) NOT NULL,
    sido VARCHAR(255) NOT NULL,
    sigungu VARCHAR(255) NOT NULL,
    dong VARCHAR(255) NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    naver_data JSONB,
    google_data JSONB,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create unique index only if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c 
        JOIN pg_namespace n ON n.oid = c.relnamespace 
        WHERE c.relname = 'idx_places_natural_key' 
        AND n.nspname = 'public'
    ) THEN
        CREATE UNIQUE INDEX idx_places_natural_key ON places (
            lower(trim(sido)),
            lower(trim(sigungu)),
            lower(trim(dong))
        );
    END IF;
END $$;

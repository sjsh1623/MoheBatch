-- V3: Migrate existing places table to match entity schema
-- This migration transforms the existing basic places table to the full schema

-- First, enable pgvector extension if not exists
CREATE EXTENSION IF NOT EXISTS vector;

-- Create MBTI enum type if not exists
DO $$ BEGIN
    CREATE TYPE mbti_type AS ENUM (
        'INTJ','INTP','ENTJ','ENTP','INFJ','INFP','ENFJ','ENFP',
        'ISTJ','ISFJ','ESTJ','ESFJ','ISTP','ISFP','ESTP','ESFP'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Add new columns to existing places table
ALTER TABLE places 
ADD COLUMN IF NOT EXISTS name VARCHAR(255),
ADD COLUMN IF NOT EXISTS title VARCHAR(255),
ADD COLUMN IF NOT EXISTS address TEXT,
ADD COLUMN IF NOT EXISTS road_address TEXT,
ADD COLUMN IF NOT EXISTS location TEXT,
ADD COLUMN IF NOT EXISTS category VARCHAR(100),
ADD COLUMN IF NOT EXISTS description TEXT,
ADD COLUMN IF NOT EXISTS image_url TEXT,
ADD COLUMN IF NOT EXISTS images TEXT[],
ADD COLUMN IF NOT EXISTS gallery TEXT[],
ADD COLUMN IF NOT EXISTS rating NUMERIC(3, 2) DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS review_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS amenities TEXT[],
ADD COLUMN IF NOT EXISTS tags TEXT[],
ADD COLUMN IF NOT EXISTS popularity INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS naver_place_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS google_place_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS phone VARCHAR(50),
ADD COLUMN IF NOT EXISTS website_url TEXT,
ADD COLUMN IF NOT EXISTS opening_hours JSONB,
ADD COLUMN IF NOT EXISTS types TEXT[],
ADD COLUMN IF NOT EXISTS user_ratings_total INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS price_level INTEGER,
ADD COLUMN IF NOT EXISTS source_flags VARCHAR(50) DEFAULT 'UNKNOWN',
ADD COLUMN IF NOT EXISTS opened_date DATE,
ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS last_rating_check TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS is_new_place BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS should_recheck_rating BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS keyword_vector TEXT,
ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();

-- Change latitude/longitude to NUMERIC with proper precision if they're not already
ALTER TABLE places 
ALTER COLUMN latitude TYPE NUMERIC(10, 8),
ALTER COLUMN longitude TYPE NUMERIC(11, 8);

-- Populate name and title from locat_name
UPDATE places SET 
    name = locat_name,
    title = locat_name,
    location = CONCAT(sido, ' ', sigungu, ' ', dong)
WHERE name IS NULL;

-- Set NOT NULL constraints on required columns
ALTER TABLE places 
ALTER COLUMN name SET NOT NULL,
ALTER COLUMN title SET NOT NULL;

-- Update created_at and updated_at for existing records if they're NULL
UPDATE places SET 
    created_at = COALESCE(updated_at, NOW()),
    first_seen_at = COALESCE(updated_at, NOW())
WHERE created_at IS NULL;

-- Alter updated_at to have proper default and NOT NULL
ALTER TABLE places 
ALTER COLUMN created_at SET NOT NULL,
ALTER COLUMN updated_at SET DEFAULT NOW(),
ALTER COLUMN updated_at SET NOT NULL;

-- Create other tables if they don't exist (copy from V1)

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    nickname VARCHAR(100) UNIQUE,
    mbti VARCHAR(4),
    age_range VARCHAR(20),
    transportation VARCHAR(50),
    profile_image_url TEXT,
    is_onboarding_completed BOOLEAN NOT NULL DEFAULT FALSE,
    last_login_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Place images table
CREATE TABLE IF NOT EXISTS place_images (
    id BIGSERIAL PRIMARY KEY,
    place_id BIGINT NOT NULL,
    image_url VARCHAR(2048) NOT NULL,
    image_type VARCHAR(50) NOT NULL DEFAULT 'GENERAL',
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    display_order INTEGER NOT NULL DEFAULT 0,
    source VARCHAR(50) NOT NULL DEFAULT 'GOOGLE_IMAGES',
    source_id VARCHAR(255),
    width INTEGER,
    height INTEGER,
    file_size BIGINT,
    alt_text TEXT,
    caption TEXT,
    is_verified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    CONSTRAINT fk_place_images_place_id 
        FOREIGN KEY (place_id) REFERENCES places(id) 
        ON DELETE CASCADE
);

-- Bookmarks table
CREATE TABLE IF NOT EXISTS bookmarks (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, place_id)
);

-- Create indexes if they don't exist
CREATE INDEX IF NOT EXISTS idx_places_category ON places(category);
CREATE INDEX IF NOT EXISTS idx_places_rating ON places(rating DESC);
CREATE INDEX IF NOT EXISTS idx_places_naver_id ON places(naver_place_id);
CREATE INDEX IF NOT EXISTS idx_places_google_id ON places(google_place_id);
CREATE INDEX IF NOT EXISTS idx_places_created_at ON places(created_at);
CREATE INDEX IF NOT EXISTS idx_places_is_new_place ON places(is_new_place);
CREATE INDEX IF NOT EXISTS idx_places_first_seen_at ON places(first_seen_at);
CREATE INDEX IF NOT EXISTS idx_places_name ON places(name);

-- Create updated_at trigger if it doesn't exist
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply updated_at triggers to relevant tables
DROP TRIGGER IF EXISTS trigger_places_updated_at ON places;
CREATE TRIGGER trigger_places_updated_at BEFORE UPDATE ON places FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert migration log entry
CREATE TABLE IF NOT EXISTS migration_log (
    id BIGSERIAL PRIMARY KEY,
    migration_version VARCHAR(20) NOT NULL,
    description TEXT,
    executed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

INSERT INTO migration_log (migration_version, description) 
VALUES ('V3', 'Migrate existing places table to match entity schema');
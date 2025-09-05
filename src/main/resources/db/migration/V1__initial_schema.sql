-- V1: Initial schema for Mohe Spring application (Java 21 migration baseline)
-- This is a clean baseline after Kotlin to Java migration

-- Enable pgvector extension for vector operations
CREATE EXTENSION IF NOT EXISTS vector;

-- Create MBTI enum type
CREATE TYPE mbti_type AS ENUM (
    'INTJ','INTP','ENTJ','ENTP','INFJ','INFP','ENFJ','ENFP',
    'ISTJ','ISFJ','ESTJ','ESFJ','ISTP','ISFP','ESTP','ESFP'
);

-- Users table
CREATE TABLE users (
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

-- Places table (clean schema matching Java entities)
CREATE TABLE places (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address TEXT,
    road_address TEXT,
    latitude NUMERIC(10, 8),
    longitude NUMERIC(11, 8),
    category VARCHAR(100),
    description TEXT,
    image_url TEXT,
    images TEXT[],
    gallery TEXT[],
    rating NUMERIC(3, 2) DEFAULT 0.0,
    review_count INTEGER DEFAULT 0,
    amenities TEXT[],
    tags TEXT[],
    popularity INTEGER DEFAULT 0,
    naver_place_id VARCHAR(255),
    google_place_id VARCHAR(255),
    phone VARCHAR(50),
    website_url TEXT,
    opening_hours JSONB,
    types TEXT[],
    user_ratings_total INTEGER DEFAULT 0,
    price_level INTEGER,
    source_flags VARCHAR(50) DEFAULT 'UNKNOWN',
    opened_date DATE,
    first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_rating_check TIMESTAMP WITH TIME ZONE,
    is_new_place BOOLEAN DEFAULT FALSE,
    should_recheck_rating BOOLEAN DEFAULT FALSE,
    keyword_vector TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Place images table
CREATE TABLE place_images (
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
        ON DELETE CASCADE,
    
    CONSTRAINT chk_image_type 
        CHECK (image_type IN ('GENERAL', 'EXTERIOR', 'INTERIOR', 'FOOD', 'MENU', 'AMBIANCE', 'DETAIL', 'PANORAMIC')),
        
    CONSTRAINT chk_image_source 
        CHECK (source IN ('GOOGLE_IMAGES', 'GOOGLE_PLACES', 'NAVER', 'MANUAL_UPLOAD', 'WEB_SCRAPING')),
        
    CONSTRAINT chk_display_order_positive 
        CHECK (display_order >= 0),
        
    -- Ensure only one primary image per place
    CONSTRAINT uq_place_primary_image 
        EXCLUDE (place_id WITH =) WHERE (is_primary = TRUE)
);

-- Bookmarks table
CREATE TABLE bookmarks (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, place_id)
);

-- Prompts table
CREATE TABLE prompts (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- User preferences table
CREATE TABLE preferences (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    pref_key VARCHAR(100) NOT NULL,
    pref_value TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Recent views table
CREATE TABLE recent_views (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    viewed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Terms agreements table
CREATE TABLE terms_agreements (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    terms_code VARCHAR(50) NOT NULL,
    agreed BOOLEAN NOT NULL DEFAULT FALSE,
    agreed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Email verifications table
CREATE TABLE email_verifications (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL,
    code VARCHAR(10) NOT NULL,
    issued_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    verified_at TIMESTAMP WITH TIME ZONE,
    success BOOLEAN DEFAULT FALSE
);

-- Place MBTI score table
CREATE TABLE place_mbti_score (
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    mbti mbti_type NOT NULL,
    score NUMERIC(5,2) NOT NULL CHECK (score >= 0 AND score <= 100),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (place_id, mbti)
);

-- JWT refresh tokens table
CREATE TABLE refresh_tokens (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token VARCHAR(512) NOT NULL UNIQUE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    is_revoked BOOLEAN DEFAULT FALSE
);

-- Temporary users table for registration process
CREATE TABLE temp_users (
    id VARCHAR(255) PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    verification_code VARCHAR(10) NOT NULL,
    nickname VARCHAR(100),
    password_hash VARCHAR(255),
    terms_agreed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Password reset tokens table
CREATE TABLE password_reset_tokens (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token VARCHAR(255) NOT NULL UNIQUE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    used BOOLEAN DEFAULT FALSE
);

-- Place similarity matrices
CREATE TABLE place_similarity (
    place1_id BIGINT NOT NULL,
    place2_id BIGINT NOT NULL,
    jaccard_score REAL,
    cosine_score REAL,
    combined_score REAL,
    calculation_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (place1_id, place2_id),
    FOREIGN KEY (place1_id) REFERENCES places(id) ON DELETE CASCADE,
    FOREIGN KEY (place2_id) REFERENCES places(id) ON DELETE CASCADE
);

-- Top-K place similarities for fast recommendations
CREATE TABLE place_similarity_topk (
    place_id BIGINT NOT NULL,
    similar_place_id BIGINT NOT NULL,
    similarity_score REAL NOT NULL,
    rank_position INTEGER NOT NULL,
    calculation_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (place_id, similar_place_id),
    FOREIGN KEY (place_id) REFERENCES places(id) ON DELETE CASCADE,
    FOREIGN KEY (similar_place_id) REFERENCES places(id) ON DELETE CASCADE
);

-- Place MBTI descriptions
CREATE TABLE place_mbti_descriptions (
    id BIGSERIAL PRIMARY KEY,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    mbti_type VARCHAR(4) NOT NULL,
    description TEXT NOT NULL,
    confidence_score NUMERIC(5,2) DEFAULT 0.0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(place_id, mbti_type)
);

-- Keyword catalog
CREATE TABLE keyword_catalog (
    id BIGSERIAL PRIMARY KEY,
    keyword VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(50) NOT NULL,
    mbti_weights JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Place keyword extraction results
CREATE TABLE place_keyword_extraction (
    id BIGSERIAL PRIMARY KEY,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    extracted_keywords JSONB NOT NULL DEFAULT '[]',
    confidence_scores JSONB NOT NULL DEFAULT '{}',
    extraction_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    model_version VARCHAR(50) DEFAULT 'unknown',
    processing_time_ms INTEGER,
    UNIQUE(place_id)
);

-- Vector similarity calculations
CREATE TABLE vector_similarity (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
    place_id BIGINT REFERENCES places(id) ON DELETE CASCADE,
    similarity_score REAL NOT NULL,
    calculation_method VARCHAR(50) NOT NULL DEFAULT 'COSINE',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- User preference vectors
CREATE TABLE user_preference_vector (
    user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    preference_vector vector(768),
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    bookmark_count INTEGER DEFAULT 0
);

-- Place description vectors
CREATE TABLE place_description_vector (
    place_id BIGINT PRIMARY KEY REFERENCES places(id) ON DELETE CASCADE,
    description_vector vector(768),
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    vector_source VARCHAR(50) DEFAULT 'MERGED_DESCRIPTION'
);

-- Place external raw data (for debugging)
CREATE TABLE place_external_raw (
    id BIGSERIAL PRIMARY KEY,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    api_source VARCHAR(50) NOT NULL,
    raw_response JSONB NOT NULL,
    fetch_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    response_code INTEGER,
    processing_notes TEXT
);

-- Migration log table
CREATE TABLE migration_log (
    id BIGSERIAL PRIMARY KEY,
    migration_version VARCHAR(20) NOT NULL,
    description TEXT,
    executed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create indexes for performance

-- Users indexes
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_nickname ON users(nickname);
CREATE INDEX idx_users_mbti ON users(mbti);

-- Places indexes
CREATE INDEX idx_places_category ON places(category);
CREATE INDEX idx_places_rating ON places(rating DESC);
CREATE INDEX idx_places_location ON places USING gist (ll_to_earth(latitude, longitude));
CREATE INDEX idx_places_naver_id ON places(naver_place_id);
CREATE INDEX idx_places_google_id ON places(google_place_id);
CREATE INDEX idx_places_created_at ON places(created_at);
CREATE INDEX idx_places_is_new_place ON places(is_new_place);
CREATE INDEX idx_places_first_seen_at ON places(first_seen_at);

-- Place images indexes
CREATE INDEX idx_place_images_place_id ON place_images(place_id);
CREATE INDEX idx_place_images_is_primary ON place_images(is_primary);
CREATE INDEX idx_place_images_display_order ON place_images(display_order);
CREATE INDEX idx_place_images_image_type ON place_images(image_type);
CREATE INDEX idx_place_images_source ON place_images(source);
CREATE INDEX idx_place_images_is_verified ON place_images(is_verified);
CREATE INDEX idx_place_images_created_at ON place_images(created_at);

-- Bookmarks indexes
CREATE INDEX idx_bookmarks_user_id ON bookmarks(user_id);
CREATE INDEX idx_bookmarks_place_id ON bookmarks(place_id);
CREATE INDEX idx_bookmarks_created_at ON bookmarks(created_at);

-- Other table indexes
CREATE INDEX idx_prompts_user_id ON prompts(user_id);
CREATE INDEX idx_prompts_place_id ON prompts(place_id);
CREATE INDEX idx_preferences_user_id ON preferences(user_id);
CREATE INDEX idx_preferences_key ON preferences(pref_key);
CREATE INDEX idx_recent_views_user_id ON recent_views(user_id);
CREATE INDEX idx_recent_views_place_id ON recent_views(place_id);
CREATE INDEX idx_recent_views_viewed_at ON recent_views(viewed_at DESC);
CREATE INDEX idx_terms_agreements_user_id ON terms_agreements(user_id);
CREATE INDEX idx_email_verifications_user_id ON email_verifications(user_id);
CREATE INDEX idx_email_verifications_code ON email_verifications(code);
CREATE INDEX idx_place_mbti_score_mbti ON place_mbti_score(mbti, score DESC);
CREATE INDEX idx_place_mbti_score_place_id ON place_mbti_score(place_id);
CREATE INDEX idx_refresh_tokens_user_id ON refresh_tokens(user_id);
CREATE INDEX idx_refresh_tokens_token ON refresh_tokens(token);
CREATE INDEX idx_refresh_tokens_expires_at ON refresh_tokens(expires_at);
CREATE INDEX idx_temp_users_email ON temp_users(email);
CREATE INDEX idx_temp_users_expires_at ON temp_users(expires_at);
CREATE INDEX idx_password_reset_tokens_token ON password_reset_tokens(token);
CREATE INDEX idx_password_reset_tokens_expires_at ON password_reset_tokens(expires_at);

-- Similarity table indexes
CREATE INDEX idx_place_similarity_place1 ON place_similarity(place1_id);
CREATE INDEX idx_place_similarity_place2 ON place_similarity(place2_id);
CREATE INDEX idx_place_similarity_combined_score ON place_similarity(combined_score DESC);
CREATE INDEX idx_place_similarity_topk_place_id ON place_similarity_topk(place_id);
CREATE INDEX idx_place_similarity_topk_score ON place_similarity_topk(similarity_score DESC);
CREATE INDEX idx_place_similarity_topk_rank ON place_similarity_topk(rank_position);

-- Vector and ML indexes
CREATE INDEX idx_place_mbti_descriptions_place_id ON place_mbti_descriptions(place_id);
CREATE INDEX idx_place_mbti_descriptions_mbti ON place_mbti_descriptions(mbti_type);
CREATE INDEX idx_keyword_catalog_category ON keyword_catalog(category);
CREATE INDEX idx_place_keyword_extraction_place_id ON place_keyword_extraction(place_id);
CREATE INDEX idx_vector_similarity_user_id ON vector_similarity(user_id);
CREATE INDEX idx_vector_similarity_place_id ON vector_similarity(place_id);
CREATE INDEX idx_vector_similarity_score ON vector_similarity(similarity_score DESC);
CREATE INDEX idx_user_preference_vector_updated ON user_preference_vector(last_updated);
CREATE INDEX idx_place_description_vector_updated ON place_description_vector(last_updated);
CREATE INDEX idx_place_external_raw_place_id ON place_external_raw(place_id);
CREATE INDEX idx_place_external_raw_source ON place_external_raw(api_source);
CREATE INDEX idx_place_external_raw_timestamp ON place_external_raw(fetch_timestamp);


CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_user_preference_vector_cosine 
    ON user_preference_vector USING ivfflat (preference_vector vector_cosine_ops) 
    WITH (lists = 100);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_place_description_vector_cosine 
    ON place_description_vector USING ivfflat (description_vector vector_cosine_ops) 
    WITH (lists = 100);

-- Create triggers for updated_at timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply updated_at triggers to relevant tables
CREATE TRIGGER trigger_users_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER trigger_places_updated_at BEFORE UPDATE ON places FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER trigger_place_images_updated_at BEFORE UPDATE ON place_images FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER trigger_place_mbti_descriptions_updated_at BEFORE UPDATE ON place_mbti_descriptions FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create trigger for description updates
CREATE OR REPLACE FUNCTION update_description_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_description_update = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER trigger_update_description_timestamp
    BEFORE UPDATE ON places
    FOR EACH ROW
    WHEN (OLD.merged_description IS DISTINCT FROM NEW.merged_description 
          OR OLD.description_style IS DISTINCT FROM NEW.description_style)
    EXECUTE FUNCTION update_description_timestamp();

-- Add table comments
COMMENT ON TABLE places IS 'Core places table with cleaned schema after Kotlin to Java migration';
COMMENT ON TABLE place_images IS 'Stores multiple images per place with metadata for better image management';
COMMENT ON TABLE place_similarity IS 'Precomputed similarity matrices for fast recommendations';
COMMENT ON TABLE place_similarity_topk IS 'Top-K similar places cache for sub-100ms lookups';
COMMENT ON TABLE vector_similarity IS 'Vector-based semantic similarity calculations';

-- Insert initial migration log entry
INSERT INTO migration_log (migration_version, description) 
VALUES ('V1', 'Initial schema baseline after Kotlin to Java 21 migration');
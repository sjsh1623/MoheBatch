-- Add new columns to places table for enhanced data
ALTER TABLE places 
ADD COLUMN IF NOT EXISTS naver_place_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS google_place_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS phone VARCHAR(50),
ADD COLUMN IF NOT EXISTS website_url VARCHAR(500),
ADD COLUMN IF NOT EXISTS opening_hours JSONB,
ADD COLUMN IF NOT EXISTS types TEXT[],
ADD COLUMN IF NOT EXISTS user_ratings_total INT DEFAULT 0,
ADD COLUMN IF NOT EXISTS price_level SMALLINT,
ADD COLUMN IF NOT EXISTS source_flags JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS road_address VARCHAR(500);

-- Create indexes for external IDs
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_places_naver_place_id ON places(naver_place_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_places_google_place_id ON places(google_place_id);

-- External raw data storage for audit
CREATE TABLE IF NOT EXISTS place_external_raw (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,  -- 'naver', 'google', etc.
    external_id VARCHAR(255) NOT NULL,
    place_id BIGINT,  -- FK to places table (nullable for failed processing)
    payload JSONB NOT NULL,
    fetched_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(source, external_id)
);

CREATE INDEX IF NOT EXISTS idx_place_external_raw_place_id ON place_external_raw(place_id);
CREATE INDEX IF NOT EXISTS idx_place_external_raw_source ON place_external_raw(source);
CREATE INDEX IF NOT EXISTS idx_place_external_raw_fetched_at ON place_external_raw(fetched_at);

-- MBTI descriptions for places
CREATE TABLE IF NOT EXISTS place_mbti_descriptions (
    id BIGSERIAL PRIMARY KEY,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    mbti VARCHAR(4) NOT NULL CHECK (mbti IN ('INTJ','INTP','ENTJ','ENTP','INFJ','INFP','ENFJ','ENFP','ISTJ','ISFJ','ESTJ','ESFJ','ISTP','ISFP','ESTP','ESFP')),
    description TEXT NOT NULL,
    model VARCHAR(100) DEFAULT 'llama3.1:latest',
    prompt_hash VARCHAR(64) NOT NULL, -- For cache invalidation
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(place_id, mbti)
);

CREATE INDEX IF NOT EXISTS idx_place_mbti_place_id ON place_mbti_descriptions(place_id);
CREATE INDEX IF NOT EXISTS idx_place_mbti_mbti ON place_mbti_descriptions(mbti);
CREATE INDEX IF NOT EXISTS idx_place_mbti_updated_at ON place_mbti_descriptions(updated_at);

-- Place similarity matrix
CREATE TABLE IF NOT EXISTS place_similarity (
    place_id1 BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    place_id2 BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    jaccard DECIMAL(5,4) DEFAULT 0.0000,
    cosine_bin DECIMAL(5,4) DEFAULT 0.0000,
    co_users INT DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (place_id1, place_id2),
    CHECK (place_id1 != place_id2),
    CHECK (place_id1 < place_id2)  -- Ensure consistent ordering
);

CREATE INDEX IF NOT EXISTS idx_place_similarity_place1 ON place_similarity(place_id1);
CREATE INDEX IF NOT EXISTS idx_place_similarity_place2 ON place_similarity(place_id2);
CREATE INDEX IF NOT EXISTS idx_place_similarity_jaccard ON place_similarity(jaccard DESC);
CREATE INDEX IF NOT EXISTS idx_place_similarity_cosine ON place_similarity(cosine_bin DESC);

-- Top-K similarity cache for fast recommendations
CREATE TABLE IF NOT EXISTS place_similarity_topk (
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    neighbor_place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    rank SMALLINT NOT NULL,
    jaccard DECIMAL(5,4) DEFAULT 0.0000,
    cosine_bin DECIMAL(5,4) DEFAULT 0.0000,
    co_users INT DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (place_id, neighbor_place_id),
    UNIQUE (place_id, rank)
);

CREATE INDEX IF NOT EXISTS idx_place_similarity_topk_place ON place_similarity_topk(place_id, rank);
CREATE INDEX IF NOT EXISTS idx_place_similarity_topk_neighbor ON place_similarity_topk(neighbor_place_id);

-- Optional: MBTI-specific similarity tables (controlled by config)
CREATE TABLE IF NOT EXISTS place_similarity_mbti_topk (
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    mbti VARCHAR(4) NOT NULL,
    neighbor_place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    rank SMALLINT NOT NULL,
    jaccard DECIMAL(5,4) DEFAULT 0.0000,
    cosine_bin DECIMAL(5,4) DEFAULT 0.0000,
    co_users INT DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (place_id, mbti, neighbor_place_id),
    UNIQUE (place_id, mbti, rank)
);

CREATE INDEX IF NOT EXISTS idx_place_similarity_mbti_topk_place_mbti ON place_similarity_mbti_topk(place_id, mbti, rank);

-- Add constraint to prevent duplicate external IDs
ALTER TABLE places ADD CONSTRAINT uq_places_naver_place_id UNIQUE (naver_place_id);
ALTER TABLE places ADD CONSTRAINT uq_places_google_place_id UNIQUE (google_place_id);
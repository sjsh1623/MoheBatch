-- Add vector extension for PostgreSQL
CREATE EXTENSION IF NOT EXISTS vector;

-- User preference vectors table
CREATE TABLE IF NOT EXISTS user_preference_vectors (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Raw text used for extraction
    raw_profile_text TEXT NOT NULL,
    combined_preferences_text TEXT,
    
    -- 100-dimensional vector (sparse representation - only non-zero values stored)
    preference_vector vector(100) NOT NULL,
    
    -- Selected keywords with confidences (exactly 15)
    selected_keywords JSONB NOT NULL, -- [{"keyword_id": 1, "keyword": "specialty_coffee", "confidence": 0.85}, ...]
    
    -- Extraction metadata
    extraction_source TEXT NOT NULL DEFAULT 'ollama-openai',
    model_name TEXT NOT NULL,
    model_version TEXT,
    extraction_prompt_hash TEXT, -- SHA-256 of prompt used
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure only one active vector per user
    UNIQUE(user_id)
);

-- Place description vectors table  
CREATE TABLE IF NOT EXISTS place_description_vectors (
    id BIGSERIAL PRIMARY KEY,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    
    -- Raw text used for extraction
    raw_description_text TEXT NOT NULL,
    combined_attributes_text TEXT, -- description + tags + amenities + category
    
    -- 100-dimensional vector (sparse representation)
    description_vector vector(100) NOT NULL,
    
    -- Selected keywords with confidences (exactly 15)
    selected_keywords JSONB NOT NULL,
    
    -- Extraction metadata
    extraction_source TEXT NOT NULL DEFAULT 'ollama-openai',
    model_name TEXT NOT NULL,
    model_version TEXT,
    extraction_prompt_hash TEXT,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure only one active vector per place
    UNIQUE(place_id)
);

-- Vector similarity cache for user-place matches
CREATE TABLE IF NOT EXISTS vector_similarities (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    
    -- Similarity scores
    cosine_similarity NUMERIC(5,4) NOT NULL CHECK (cosine_similarity >= -1 AND cosine_similarity <= 1),
    euclidean_distance NUMERIC(8,4) NOT NULL CHECK (euclidean_distance >= 0),
    jaccard_similarity NUMERIC(5,4) CHECK (jaccard_similarity >= 0 AND jaccard_similarity <= 1),
    
    -- MBTI-weighted similarity boost
    mbti_boost_factor NUMERIC(3,2) DEFAULT 1.0,
    weighted_similarity NUMERIC(5,4) NOT NULL, -- Final score after MBTI weighting
    
    -- Keyword overlap details
    common_keywords INTEGER NOT NULL DEFAULT 0,
    keyword_overlap_ratio NUMERIC(3,2),
    
    -- Metadata
    calculated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    user_vector_version BIGINT, -- References user_preference_vectors.id
    place_vector_version BIGINT, -- References place_description_vectors.id
    
    PRIMARY KEY (user_id, place_id)
);

-- Keyword embedding cache for semantic similarity
CREATE TABLE IF NOT EXISTS keyword_embeddings (
    id SERIAL PRIMARY KEY,
    keyword_id INTEGER NOT NULL,
    keyword_name TEXT NOT NULL,
    
    -- Semantic embedding vector (can be different dimension, e.g., 384 for sentence transformers)
    semantic_vector vector(384),
    
    -- Metadata
    embedding_model TEXT NOT NULL DEFAULT 'sentence-transformers',
    embedding_version TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(keyword_id, embedding_model)
);

-- Vector search performance indices
-- IVFFLAT index for approximate nearest neighbor search on user preference vectors
CREATE INDEX IF NOT EXISTS idx_user_preference_vectors_ivfflat 
ON user_preference_vectors USING ivfflat (preference_vector vector_cosine_ops)
WITH (lists = 100);

-- IVFFLAT index for place description vectors  
CREATE INDEX IF NOT EXISTS idx_place_description_vectors_ivfflat
ON place_description_vectors USING ivfflat (description_vector vector_cosine_ops)
WITH (lists = 100);

-- Standard B-tree indices for foreign keys and queries
CREATE INDEX IF NOT EXISTS idx_user_preference_vectors_user_id ON user_preference_vectors(user_id);
CREATE INDEX IF NOT EXISTS idx_place_description_vectors_place_id ON place_description_vectors(place_id);
CREATE INDEX IF NOT EXISTS idx_vector_similarities_user_id ON vector_similarities(user_id);
CREATE INDEX IF NOT EXISTS idx_vector_similarities_place_id ON vector_similarities(place_id);
CREATE INDEX IF NOT EXISTS idx_vector_similarities_weighted_score ON vector_similarities(weighted_similarity DESC);
CREATE INDEX IF NOT EXISTS idx_vector_similarities_calculated_at ON vector_similarities(calculated_at);

-- GIN index for JSONB keyword searches
CREATE INDEX IF NOT EXISTS idx_user_preference_keywords_gin ON user_preference_vectors USING gin(selected_keywords);
CREATE INDEX IF NOT EXISTS idx_place_description_keywords_gin ON place_description_vectors USING gin(selected_keywords);

-- Functional index for keyword extraction metadata
CREATE INDEX IF NOT EXISTS idx_user_vectors_extraction_source ON user_preference_vectors(extraction_source, model_name);
CREATE INDEX IF NOT EXISTS idx_place_vectors_extraction_source ON place_description_vectors(extraction_source, model_name);

-- Semantic similarity index for keyword embeddings
CREATE INDEX IF NOT EXISTS idx_keyword_embeddings_semantic_ivfflat
ON keyword_embeddings USING ivfflat (semantic_vector vector_cosine_ops)
WITH (lists = 50);

-- Add trigger to update vector_similarities when vectors change
CREATE OR REPLACE FUNCTION update_vector_similarity_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    
    -- Invalidate related similarity cache entries
    DELETE FROM vector_similarities 
    WHERE (TG_TABLE_NAME = 'user_preference_vectors' AND user_id = NEW.user_id)
       OR (TG_TABLE_NAME = 'place_description_vectors' AND place_id = NEW.place_id);
       
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_user_preference_vectors_updated_at
    BEFORE UPDATE ON user_preference_vectors
    FOR EACH ROW
    EXECUTE FUNCTION update_vector_similarity_timestamp();

CREATE TRIGGER trigger_place_description_vectors_updated_at  
    BEFORE UPDATE ON place_description_vectors
    FOR EACH ROW
    EXECUTE FUNCTION update_vector_similarity_timestamp();

-- Function to calculate vector similarity with MBTI weighting
CREATE OR REPLACE FUNCTION calculate_weighted_vector_similarity(
    user_vector vector(100),
    place_vector vector(100), 
    user_mbti TEXT,
    place_category TEXT,
    mbti_boost_multiplier NUMERIC DEFAULT 1.2
) RETURNS NUMERIC AS $$
DECLARE
    base_similarity NUMERIC;
    mbti_boost NUMERIC DEFAULT 1.0;
    final_score NUMERIC;
BEGIN
    -- Calculate cosine similarity
    base_similarity := 1 - (user_vector <=> place_vector);
    
    -- Apply MBTI-based category boost
    IF user_mbti IS NOT NULL THEN
        -- Example: Introverts get boost for quiet places, Extraverts for social places
        CASE 
            WHEN user_mbti LIKE 'I%' AND place_category IN ('quiet_space', 'study_friendly', 'bookstore_cafe') THEN
                mbti_boost := mbti_boost_multiplier;
            WHEN user_mbti LIKE 'E%' AND place_category IN ('social_buzzing', 'group_seating', 'community_events') THEN
                mbti_boost := mbti_boost_multiplier;
            WHEN user_mbti LIKE '%N%' AND place_category IN ('artsy_creative', 'themed_concept', 'hidden_gem') THEN
                mbti_boost := mbti_boost_multiplier;
            WHEN user_mbti LIKE '%S%' AND place_category IN ('traditional_korean', 'established_legacy', 'reliable_service') THEN
                mbti_boost := mbti_boost_multiplier;
            ELSE
                mbti_boost := 1.0;
        END CASE;
    END IF;
    
    final_score := base_similarity * mbti_boost;
    RETURN LEAST(1.0, final_score); -- Cap at 1.0
END;
$$ LANGUAGE plpgsql;
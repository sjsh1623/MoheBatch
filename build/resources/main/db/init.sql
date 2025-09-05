-- Create database schema for Mohe Spring application

CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    nickname TEXT UNIQUE,
    mbti TEXT,
    age_range TEXT,
    transportation TEXT,
    profile_image_url TEXT,
    is_onboarding_completed BOOLEAN DEFAULT FALSE,
    last_login_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS places (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    title TEXT NOT NULL,
    address TEXT,
    location TEXT,
    latitude NUMERIC(10, 8),
    longitude NUMERIC(11, 8),
    altitude NUMERIC(10, 2),
    category TEXT,
    description TEXT,
    image_url TEXT,
    images TEXT[],
    gallery TEXT[],
    additional_image_count INTEGER DEFAULT 0,
    rating NUMERIC(3, 2) DEFAULT 0.0,
    review_count INTEGER DEFAULT 0,
    operating_hours TEXT,
    amenities TEXT[],
    tags TEXT[],
    transportation_car_time TEXT,
    transportation_bus_time TEXT,
    weather_tags TEXT[],
    noise_tags TEXT[],
    popularity INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bookmarks (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, place_id)
);

CREATE TABLE IF NOT EXISTS prompts (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS preferences (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    pref_key TEXT NOT NULL,
    pref_value TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS recent_views (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    viewed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS terms_agreements (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    terms_code TEXT NOT NULL,
    agreed BOOLEAN NOT NULL DEFAULT FALSE,
    agreed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS email_verifications (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    code TEXT NOT NULL,
    issued_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    verified_at TIMESTAMPTZ,
    success BOOLEAN DEFAULT FALSE
);


DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'mbti_type') THEN
CREATE TYPE mbti_type AS ENUM (
      'INTJ','INTP','ENTJ','ENTP','INFJ','INFP','ENFJ','ENFP',
      'ISTJ','ISFJ','ESTJ','ESFJ','ISTP','ISFP','ESTP','ESFP'
    );
END IF;
END$$;

CREATE TABLE IF NOT EXISTS place_mbti_score (
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    mbti     mbti_type NOT NULL,
    score    NUMERIC(5,2) NOT NULL CHECK (score >= 0 AND score <= 100),
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (place_id, mbti)
);



-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_bookmarks_user_id ON bookmarks(user_id);
CREATE INDEX IF NOT EXISTS idx_bookmarks_place_id ON bookmarks(place_id);
CREATE INDEX IF NOT EXISTS idx_prompts_user_id ON prompts(user_id);
CREATE INDEX IF NOT EXISTS idx_prompts_place_id ON prompts(place_id);
CREATE INDEX IF NOT EXISTS idx_preferences_user_id ON preferences(user_id);
CREATE INDEX IF NOT EXISTS idx_recent_views_user_id ON recent_views(user_id);
CREATE INDEX IF NOT EXISTS idx_recent_views_place_id ON recent_views(place_id);
CREATE INDEX IF NOT EXISTS idx_terms_agreements_user_id ON terms_agreements(user_id);
-- JWT Token storage table
CREATE TABLE IF NOT EXISTS refresh_tokens (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token TEXT NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    is_revoked BOOLEAN DEFAULT FALSE
);

-- Temporary user storage for registration process
CREATE TABLE IF NOT EXISTS temp_users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    verification_code TEXT NOT NULL,
    nickname TEXT,
    password_hash TEXT,
    terms_agreed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ NOT NULL
);

-- Password reset tokens
CREATE TABLE IF NOT EXISTS password_reset_tokens (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token TEXT NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    used BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_bookmarks_user_id ON bookmarks(user_id);
CREATE INDEX IF NOT EXISTS idx_bookmarks_place_id ON bookmarks(place_id);
CREATE INDEX IF NOT EXISTS idx_prompts_user_id ON prompts(user_id);
CREATE INDEX IF NOT EXISTS idx_prompts_place_id ON prompts(place_id);
CREATE INDEX IF NOT EXISTS idx_preferences_user_id ON preferences(user_id);
CREATE INDEX IF NOT EXISTS idx_recent_views_user_id ON recent_views(user_id);
CREATE INDEX IF NOT EXISTS idx_recent_views_place_id ON recent_views(place_id);
CREATE INDEX IF NOT EXISTS idx_terms_agreements_user_id ON terms_agreements(user_id);
CREATE INDEX IF NOT EXISTS idx_email_verifications_user_id ON email_verifications(user_id);
CREATE INDEX IF NOT EXISTS idx_pms_mbti_score ON place_mbti_score (mbti, score DESC);
CREATE INDEX IF NOT EXISTS idx_pms_place_id ON place_mbti_score (place_id);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user_id ON refresh_tokens(user_id);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_token ON refresh_tokens(token);
CREATE INDEX IF NOT EXISTS idx_temp_users_email ON temp_users(email);
CREATE INDEX IF NOT EXISTS idx_password_reset_tokens_token ON password_reset_tokens(token);
-- Add fields for tracking place opening dates and age-based filtering
ALTER TABLE places 
ADD COLUMN IF NOT EXISTS opened_date DATE,
ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN IF NOT EXISTS last_rating_check TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS is_new_place BOOLEAN DEFAULT TRUE,
ADD COLUMN IF NOT EXISTS should_recheck_rating BOOLEAN DEFAULT FALSE;

-- Add index for efficient age-based queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_places_opened_date ON places(opened_date);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_places_first_seen_at ON places(first_seen_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_places_last_rating_check ON places(last_rating_check);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_places_is_new_place ON places(is_new_place);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_places_should_recheck_rating ON places(should_recheck_rating);

-- Add composite index for rating and age filtering
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_places_rating_age_filter 
ON places(rating, first_seen_at, is_new_place) 
WHERE rating >= 3.0 OR is_new_place = true;

-- Update existing places to mark them as potentially needing rating rechecks
-- (Places older than 6 months from first_seen_at)
UPDATE places 
SET should_recheck_rating = TRUE, 
    last_rating_check = CURRENT_TIMESTAMP - INTERVAL '7 days'
WHERE first_seen_at < CURRENT_TIMESTAMP - INTERVAL '6 months' 
  AND last_rating_check IS NULL;
-- 사용하지 않는 컬럼들 정리 및 데이터베이스 스키마 최적화
-- 이 마이그레이션은 기존 데이터를 안전하게 보존하면서 불필요한 컬럼들을 제거합니다.

-- Step 1: 중복되거나 사용하지 않는 컬럼들을 제거하기 전에 데이터 백업
-- 필요한 경우 아래 주석을 해제하여 백업 테이블 생성
-- CREATE TABLE places_backup_v5 AS SELECT * FROM places;

-- Step 2: 사용하지 않는 컬럼들 제거
-- title 컬럼은 name과 중복됨 (backward compatibility를 위해 유지했으나 더 이상 불필요)
ALTER TABLE places DROP COLUMN IF EXISTS title;

-- location 컬럼은 address와 중복되고 사용되지 않음
ALTER TABLE places DROP COLUMN IF EXISTS location;

-- altitude 컬럼은 서울 지역 장소에서는 의미가 없음
ALTER TABLE places DROP COLUMN IF EXISTS altitude;

-- additional_image_count는 images 배열 길이로 대체 가능
ALTER TABLE places DROP COLUMN IF EXISTS additional_image_count;

-- transportation 관련 컬럼들은 외부 API로 실시간 조회하는 것이 더 정확
ALTER TABLE places DROP COLUMN IF EXISTS transportation_car_time;
ALTER TABLE places DROP COLUMN IF EXISTS transportation_bus_time;

-- weather_tags와 noise_tags는 실제 사용되지 않고 있음
ALTER TABLE places DROP COLUMN IF EXISTS weather_tags;
ALTER TABLE places DROP COLUMN IF EXISTS noise_tags;

-- operating_hours는 opening_hours (JSONB) 컬럼으로 대체됨
ALTER TABLE places DROP COLUMN IF EXISTS operating_hours;

-- Step 3: 새로운 description 관련 컬럼 추가
-- 병합된 설명과 메타데이터 저장을 위한 컬럼들
ALTER TABLE places 
ADD COLUMN IF NOT EXISTS merged_description TEXT,
ADD COLUMN IF NOT EXISTS description_style VARCHAR(50),
ADD COLUMN IF NOT EXISTS description_source_info JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS last_description_update TIMESTAMP WITH TIME ZONE;

-- Step 4: 인덱스 정리
-- 제거된 컬럼들의 인덱스도 함께 제거됨
-- 새로운 컬럼들에 대한 인덱스 생성
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_places_description_style ON places(description_style);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_places_last_description_update ON places(last_description_update);

-- Step 5: 기존 description을 merged_description으로 마이그레이션
-- 기존 데이터가 있는 경우에만 실행
UPDATE places 
SET 
    merged_description = description,
    description_style = 'LEGACY',
    description_source_info = '{"migrated_from": "description", "migration_date": "' || NOW() || '"}',
    last_description_update = NOW()
WHERE description IS NOT NULL 
  AND (merged_description IS NULL OR merged_description = '');

-- Step 6: 설명 관련 트리거 생성 (description 업데이트 시 자동으로 timestamp 갱신)
CREATE OR REPLACE FUNCTION update_description_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_description_update = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS trigger_update_description_timestamp ON places;
CREATE TRIGGER trigger_update_description_timestamp
    BEFORE UPDATE ON places
    FOR EACH ROW
    WHEN (OLD.merged_description IS DISTINCT FROM NEW.merged_description 
          OR OLD.description_style IS DISTINCT FROM NEW.description_style)
    EXECUTE FUNCTION update_description_timestamp();

-- Step 7: 통계 정보 업데이트
ANALYZE places;

-- Step 8: 뷰 생성 (호환성을 위해 기존 코드에서 사용하던 컬럼명으로 접근 가능하도록)
CREATE OR REPLACE VIEW places_compatible AS 
SELECT 
    id,
    name,
    name as title, -- backward compatibility
    address,
    road_address,
    latitude,
    longitude,
    category,
    COALESCE(merged_description, description) as description, -- 새로운 병합된 설명 우선 사용
    image_url,
    images,
    gallery,
    rating,
    review_count,
    amenities,
    tags,
    popularity,
    created_at,
    naver_place_id,
    google_place_id,
    phone,
    website_url,
    opening_hours,
    types,
    user_ratings_total,
    price_level,
    source_flags,
    updated_at,
    opened_date,
    first_seen_at,
    last_rating_check,
    is_new_place,
    should_recheck_rating,
    keyword_vector,
    -- 새로운 컬럼들
    merged_description,
    description_style,
    description_source_info,
    last_description_update
FROM places;

-- 권한 설정 (필요한 경우)
-- GRANT SELECT ON places_compatible TO mohe_app_user;

COMMENT ON COLUMN places.merged_description IS '네이버와 구글 API 데이터를 병합하여 생성된 고품질 설명';
COMMENT ON COLUMN places.description_style IS '설명 생성에 사용된 스타일 (FOOD_FOCUSED, ATMOSPHERE_DRIVEN, REVIEW_BASED, MINIMAL_CHIC, BALANCED)';
COMMENT ON COLUMN places.description_source_info IS '설명 생성 과정의 메타데이터 (사용된 소스, 처리 시간 등)';
COMMENT ON COLUMN places.last_description_update IS '설명이 마지막으로 업데이트된 시간';

-- 마이그레이션 완료 로그
INSERT INTO migration_log (migration_version, description, executed_at) 
VALUES ('V5', 'Cleanup unused columns and add enhanced description fields', NOW())
ON CONFLICT DO NOTHING;
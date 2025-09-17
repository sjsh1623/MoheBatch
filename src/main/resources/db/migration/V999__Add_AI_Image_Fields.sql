-- AI 이미지 생성 관련 필드 추가
ALTER TABLE place_images
ADD COLUMN IF NOT EXISTS is_ai_generated BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS ai_model VARCHAR(255),
ADD COLUMN IF NOT EXISTS prompt_used TEXT;

-- 기존 데이터에 대한 기본값 설정
UPDATE place_images
SET is_ai_generated = false
WHERE is_ai_generated IS NULL;

-- 인덱스 추가 (AI 생성 이미지 빠른 검색을 위해)
CREATE INDEX IF NOT EXISTS idx_place_images_ai_generated
ON place_images(is_ai_generated);

CREATE INDEX IF NOT EXISTS idx_place_images_ai_model
ON place_images(ai_model);

-- AI 생성 이미지 통계를 위한 뷰 생성 (선택적)
CREATE OR REPLACE VIEW ai_image_stats AS
SELECT
    COUNT(*) as total_images,
    COUNT(CASE WHEN is_ai_generated = true THEN 1 END) as ai_generated_count,
    COUNT(CASE WHEN is_ai_generated = false THEN 1 END) as regular_image_count,
    ROUND((COUNT(CASE WHEN is_ai_generated = true THEN 1 END) * 100.0 / COUNT(*)), 2) as ai_percentage
FROM place_images;

-- 댓글 추가
COMMENT ON COLUMN place_images.is_ai_generated IS 'AI에 의해 생성된 이미지인지 여부';
COMMENT ON COLUMN place_images.ai_model IS 'AI 이미지 생성에 사용된 모델명 (예: dall-e-3)';
COMMENT ON COLUMN place_images.prompt_used IS 'AI 이미지 생성에 사용된 프롬프트';
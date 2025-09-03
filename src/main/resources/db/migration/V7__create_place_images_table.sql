-- V7: Create dedicated place_images table for better image management
-- This allows storing 5+ images per place with proper metadata

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

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_place_images_place_id ON place_images(place_id);
CREATE INDEX IF NOT EXISTS idx_place_images_is_primary ON place_images(is_primary);
CREATE INDEX IF NOT EXISTS idx_place_images_display_order ON place_images(display_order);
CREATE INDEX IF NOT EXISTS idx_place_images_image_type ON place_images(image_type);
CREATE INDEX IF NOT EXISTS idx_place_images_source ON place_images(source);
CREATE INDEX IF NOT EXISTS idx_place_images_is_verified ON place_images(is_verified);
CREATE INDEX IF NOT EXISTS idx_place_images_created_at ON place_images(created_at);

-- Add trigger to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_place_images_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER trigger_place_images_updated_at
    BEFORE UPDATE ON place_images
    FOR EACH ROW
    EXECUTE FUNCTION update_place_images_updated_at();

-- Migrate existing image data from places table to place_images table
-- This preserves existing data while adding the new structure
INSERT INTO place_images (place_id, image_url, is_primary, display_order, source, image_type, is_verified)
SELECT 
    p.id as place_id,
    p.image_url as image_url,
    TRUE as is_primary,  -- First image becomes primary
    0 as display_order,
    'GOOGLE_PLACES' as source,
    'GENERAL' as image_type,
    FALSE as is_verified
FROM places p 
WHERE p.image_url IS NOT NULL 
  AND p.image_url != ''
  AND NOT EXISTS (
    SELECT 1 FROM place_images pi WHERE pi.place_id = p.id AND pi.is_primary = TRUE
  );

-- Migrate images from the images array field
INSERT INTO place_images (place_id, image_url, is_primary, display_order, source, image_type, is_verified)
SELECT 
    p.id as place_id,
    unnest(p.images) as image_url,
    FALSE as is_primary,  -- Array images are not primary
    ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY ordinality) as display_order,
    'GOOGLE_PLACES' as source,
    'GENERAL' as image_type,
    FALSE as is_verified
FROM places p, unnest(p.images) WITH ORDINALITY
WHERE array_length(p.images, 1) > 0
  AND NOT EXISTS (
    SELECT 1 FROM place_images pi 
    WHERE pi.place_id = p.id 
    AND pi.image_url = unnest(p.images)
  );

-- Migrate gallery images
INSERT INTO place_images (place_id, image_url, is_primary, display_order, source, image_type, is_verified)
SELECT 
    p.id as place_id,
    unnest(p.gallery) as image_url,
    FALSE as is_primary,
    (ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY ordinality) + 100) as display_order, -- Offset to avoid conflicts
    'GOOGLE_PLACES' as source,
    'AMBIANCE' as image_type, -- Gallery images are typically ambiance shots
    FALSE as is_verified
FROM places p, unnest(p.gallery) WITH ORDINALITY
WHERE array_length(p.gallery, 1) > 0
  AND NOT EXISTS (
    SELECT 1 FROM place_images pi 
    WHERE pi.place_id = p.id 
    AND pi.image_url = unnest(p.gallery)
  );

-- Add comment to document the purpose of this table
COMMENT ON TABLE place_images IS 'Stores multiple images per place with metadata for better image management. Supports 5+ images per place with categorization and verification.';
COMMENT ON COLUMN place_images.image_type IS 'Category of image: GENERAL, EXTERIOR, INTERIOR, FOOD, MENU, AMBIANCE, DETAIL, PANORAMIC';
COMMENT ON COLUMN place_images.source IS 'Source of the image: GOOGLE_IMAGES, GOOGLE_PLACES, NAVER, MANUAL_UPLOAD, WEB_SCRAPING';
COMMENT ON COLUMN place_images.is_verified IS 'Whether the image has been manually verified for quality and relevance';
COMMENT ON COLUMN place_images.display_order IS 'Order in which images should be displayed (0-based)';
-- Add pgvector extension for vector similarity search
CREATE EXTENSION IF NOT EXISTS vector;

-- Create keyword catalog table
CREATE TABLE IF NOT EXISTS keyword_catalog (
    id SERIAL PRIMARY KEY,
    keyword VARCHAR(100) NOT NULL UNIQUE,
    definition TEXT NOT NULL,
    category VARCHAR(50) NOT NULL,
    related_groups TEXT[], -- Array of related group names
    vector_position INTEGER NOT NULL, -- Position in the 100-dimensional vector (0-99)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for keyword catalog
CREATE INDEX IF NOT EXISTS idx_keyword_catalog_keyword ON keyword_catalog(keyword);
CREATE INDEX IF NOT EXISTS idx_keyword_catalog_category ON keyword_catalog(category);
CREATE INDEX IF NOT EXISTS idx_keyword_catalog_vector_position ON keyword_catalog(vector_position);

-- Create place keyword extractions table
CREATE TABLE IF NOT EXISTS place_keyword_extractions (
    id BIGSERIAL PRIMARY KEY,
    place_id BIGINT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    
    -- Raw text data
    raw_text TEXT NOT NULL, -- Original place description used for extraction
    
    -- Model information
    model_name VARCHAR(100) NOT NULL, -- e.g., "ollama-openai"
    model_version VARCHAR(50) NOT NULL, -- e.g., "llama3.1:latest"
    
    -- Vector representation (100 dimensions)
    keyword_vector vector(100) NOT NULL, -- pgvector column for similarity search
    
    -- Extracted keywords with confidence scores
    selected_keywords JSONB NOT NULL, -- Array of {keyword, confidence_score} objects
    
    -- Processing metadata
    extraction_method VARCHAR(50) DEFAULT 'ollama_llm',
    processing_time_ms INTEGER,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for place keyword extractions
CREATE INDEX IF NOT EXISTS idx_place_keyword_extractions_place_id ON place_keyword_extractions(place_id);
CREATE INDEX IF NOT EXISTS idx_place_keyword_extractions_model ON place_keyword_extractions(model_name, model_version);
CREATE INDEX IF NOT EXISTS idx_place_keyword_extractions_created_at ON place_keyword_extractions(created_at);

-- Create vector similarity index using IVFFLAT with cosine distance
CREATE INDEX IF NOT EXISTS idx_place_keyword_vector_cosine 
ON place_keyword_extractions 
USING ivfflat (keyword_vector vector_cosine_ops)
WITH (lists = 100); -- Adjust lists parameter based on data size

-- Create additional index for L2 distance if needed
CREATE INDEX IF NOT EXISTS idx_place_keyword_vector_l2 
ON place_keyword_extractions 
USING ivfflat (keyword_vector vector_l2_ops)
WITH (lists = 100);

-- Create keyword search index on selected_keywords JSONB
CREATE INDEX IF NOT EXISTS idx_place_keyword_extractions_keywords 
ON place_keyword_extractions 
USING GIN (selected_keywords);

-- Create table for keyword relationships and similarities
CREATE TABLE IF NOT EXISTS keyword_relationships (
    id SERIAL PRIMARY KEY,
    keyword1_id INTEGER NOT NULL REFERENCES keyword_catalog(id),
    keyword2_id INTEGER NOT NULL REFERENCES keyword_catalog(id),
    similarity_score DECIMAL(4,3) NOT NULL CHECK (similarity_score >= 0.0 AND similarity_score <= 1.0),
    relationship_type VARCHAR(50) NOT NULL, -- e.g., 'semantic', 'category', 'contextual'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(keyword1_id, keyword2_id),
    CHECK (keyword1_id != keyword2_id)
);

CREATE INDEX IF NOT EXISTS idx_keyword_relationships_keyword1 ON keyword_relationships(keyword1_id);
CREATE INDEX IF NOT EXISTS idx_keyword_relationships_keyword2 ON keyword_relationships(keyword2_id);
CREATE INDEX IF NOT EXISTS idx_keyword_relationships_similarity ON keyword_relationships(similarity_score DESC);

-- Insert the 100-keyword catalog
INSERT INTO keyword_catalog (keyword, definition, category, related_groups, vector_position) VALUES
-- Atmosphere (positions 0-9)
('cozy', 'Warm, comfortable, and inviting atmosphere', 'atmosphere', ARRAY['comfort', 'relaxation'], 0),
('modern', 'Contemporary design and facilities', 'atmosphere', ARRAY['design', 'trendy'], 1),
('vintage', 'Classic or retro styling and ambiance', 'atmosphere', ARRAY['design', 'nostalgic'], 2),
('quiet', 'Peaceful environment with low noise levels', 'atmosphere', ARRAY['relaxation', 'focus'], 3),
('lively', 'Energetic and vibrant atmosphere', 'atmosphere', ARRAY['social', 'entertainment'], 4),
('romantic', 'Intimate setting suitable for couples', 'atmosphere', ARRAY['intimate', 'special_occasion'], 5),
('spacious', 'Large area with ample room', 'atmosphere', ARRAY['comfort', 'layout'], 6),
('intimate', 'Small, personal, and cozy setting', 'atmosphere', ARRAY['romantic', 'private'], 7),
('bright', 'Well-lit with natural or artificial lighting', 'atmosphere', ARRAY['lighting', 'welcoming'], 8),
('dim', 'Low lighting creating a moody atmosphere', 'atmosphere', ARRAY['lighting', 'relaxation'], 9),

-- Food & Beverage (positions 10-19)
('coffee', 'Quality coffee and espresso drinks', 'food_beverage', ARRAY['beverages', 'cafe'], 10),
('tea', 'Various tea selections and preparations', 'food_beverage', ARRAY['beverages', 'relaxation'], 11),
('dessert', 'Sweet treats and dessert options', 'food_beverage', ARRAY['food', 'indulgence'], 12),
('pastry', 'Fresh baked goods and pastries', 'food_beverage', ARRAY['food', 'bakery'], 13),
('brunch', 'Late morning meal options', 'food_beverage', ARRAY['food', 'casual_dining'], 14),
('cocktail', 'Mixed alcoholic beverages', 'food_beverage', ARRAY['beverages', 'nightlife'], 15),
('wine', 'Wine selection and pairings', 'food_beverage', ARRAY['beverages', 'sophisticated'], 16),
('beer', 'Beer varieties and pub atmosphere', 'food_beverage', ARRAY['beverages', 'casual'], 17),
('healthy', 'Nutritious and health-conscious options', 'food_beverage', ARRAY['food', 'wellness'], 18),
('organic', 'Organic and natural ingredients', 'food_beverage', ARRAY['food', 'sustainable'], 19),

-- Service & Amenities (positions 20-29)
('wifi', 'Free wireless internet access', 'service_amenities', ARRAY['technology', 'work_friendly'], 20),
('parking', 'Available parking spaces', 'service_amenities', ARRAY['convenience', 'accessibility'], 21),
('takeout', 'Food and drinks available for takeaway', 'service_amenities', ARRAY['convenience', 'service'], 22),
('delivery', 'Delivery service available', 'service_amenities', ARRAY['convenience', 'service'], 23),
('reservations', 'Advance booking accepted', 'service_amenities', ARRAY['service', 'planning'], 24),
('pet_friendly', 'Pets welcome in the establishment', 'service_amenities', ARRAY['inclusive', 'lifestyle'], 25),
('wheelchair_accessible', 'Accessible for people with mobility needs', 'service_amenities', ARRAY['inclusive', 'accessibility'], 26),
('outdoor_seating', 'Tables and seating available outside', 'service_amenities', ARRAY['seating', 'fresh_air'], 27),
('private_rooms', 'Separate spaces for privacy', 'service_amenities', ARRAY['privacy', 'meetings'], 28),
('charging_stations', 'Device charging facilities available', 'service_amenities', ARRAY['technology', 'convenience'], 29),

-- Activities (positions 30-39)
('live_music', 'Regular live musical performances', 'activities', ARRAY['entertainment', 'culture'], 30),
('art_gallery', 'Art displays and exhibitions', 'activities', ARRAY['culture', 'visual_arts'], 31),
('book_reading', 'Suitable for reading and quiet activities', 'activities', ARRAY['quiet', 'intellectual'], 32),
('board_games', 'Board games available for customers', 'activities', ARRAY['entertainment', 'social'], 33),
('workshops', 'Educational or creative workshops offered', 'activities', ARRAY['learning', 'creativity'], 34),
('meetings', 'Suitable for business meetings', 'activities', ARRAY['business', 'professional'], 35),
('study', 'Good environment for studying', 'activities', ARRAY['quiet', 'focus', 'academic'], 36),
('socializing', 'Great for meeting friends and social interaction', 'activities', ARRAY['social', 'networking'], 37),
('people_watching', 'Good location for observing street life', 'activities', ARRAY['entertainment', 'relaxation'], 38),
('events', 'Special events and gatherings hosted', 'activities', ARRAY['entertainment', 'community'], 39),

-- Location & Accessibility (positions 40-49)
('downtown', 'Located in the city center', 'location_accessibility', ARRAY['location', 'urban'], 40),
('subway_nearby', 'Close to subway/metro stations', 'location_accessibility', ARRAY['transportation', 'accessibility'], 41),
('bus_stop', 'Near public bus transportation', 'location_accessibility', ARRAY['transportation', 'public_transit'], 42),
('walking_distance', 'Easily walkable from major attractions', 'location_accessibility', ARRAY['accessibility', 'pedestrian'], 43),
('tourist_area', 'Located in popular tourist district', 'location_accessibility', ARRAY['location', 'tourism'], 44),
('residential', 'In a residential neighborhood', 'location_accessibility', ARRAY['location', 'local'], 45),
('business_district', 'Located in commercial/business area', 'location_accessibility', ARRAY['location', 'professional'], 46),
('university_area', 'Near universities or colleges', 'location_accessibility', ARRAY['location', 'academic', 'young'], 47),
('riverside', 'Located near water/river', 'location_accessibility', ARRAY['location', 'scenic', 'nature'], 48),
('mountain_view', 'Views of mountains or hills', 'location_accessibility', ARRAY['location', 'scenic', 'nature'], 49),

-- Price & Value (positions 50-54)
('affordable', 'Budget-friendly pricing', 'price_value', ARRAY['price', 'value'], 50),
('expensive', 'Premium pricing category', 'price_value', ARRAY['price', 'luxury'], 51),
('mid_range', 'Moderate pricing', 'price_value', ARRAY['price', 'balanced'], 52),
('value_for_money', 'Good quality for the price paid', 'price_value', ARRAY['value', 'satisfaction'], 53),
('luxury', 'High-end, premium experience', 'price_value', ARRAY['expensive', 'quality', 'sophisticated'], 54),

-- Additional keywords (positions 55-99) - extending to complete the 100-keyword catalog
('trendy', 'Following current fashion and popularity', 'atmosphere', ARRAY['modern', 'popular'], 55),
('traditional', 'Classic and conventional style', 'atmosphere', ARRAY['vintage', 'heritage'], 56),
('minimalist', 'Simple and uncluttered design', 'atmosphere', ARRAY['modern', 'clean'], 57),
('industrial', 'Raw, urban industrial aesthetic', 'atmosphere', ARRAY['modern', 'urban'], 58),
('bohemian', 'Artistic and unconventional style', 'atmosphere', ARRAY['artistic', 'creative'], 59),
('elegant', 'Refined and sophisticated appearance', 'atmosphere', ARRAY['sophisticated', 'classy'], 60),
('rustic', 'Rural or countryside charm', 'atmosphere', ARRAY['traditional', 'natural'], 61),
('contemporary', 'Current and up-to-date style', 'atmosphere', ARRAY['modern', 'trendy'], 62),
('artistic', 'Creative and aesthetically focused', 'atmosphere', ARRAY['creative', 'culture'], 63),
('sophisticated', 'Refined and cultured environment', 'atmosphere', ARRAY['elegant', 'upscale'], 64),
('casual', 'Relaxed and informal atmosphere', 'atmosphere', ARRAY['comfortable', 'easy_going'], 65),
('upscale', 'High-quality and expensive', 'atmosphere', ARRAY['luxury', 'sophisticated'], 66),
('family_friendly', 'Suitable for families with children', 'service_amenities', ARRAY['inclusive', 'children'], 67),
('romantic_lighting', 'Mood lighting for romantic atmosphere', 'atmosphere', ARRAY['romantic', 'dim'], 68),
('natural_light', 'Abundant daylight and windows', 'atmosphere', ARRAY['bright', 'welcoming'], 69),
('music_venue', 'Regular musical performances and events', 'activities', ARRAY['live_music', 'entertainment'], 70),
('late_night', 'Open late hours for night owls', 'service_amenities', ARRAY['nightlife', 'convenient'], 71),
('early_morning', 'Opens early for morning customers', 'service_amenities', ARRAY['convenient', 'breakfast'], 72),
('seasonal_menu', 'Menu changes with seasons', 'food_beverage', ARRAY['fresh', 'variety'], 73),
('local_ingredients', 'Uses locally sourced ingredients', 'food_beverage', ARRAY['sustainable', 'fresh'], 74),
('vegan_options', 'Plant-based food choices available', 'food_beverage', ARRAY['healthy', 'inclusive'], 75),
('gluten_free', 'Gluten-free options available', 'food_beverage', ARRAY['healthy', 'inclusive'], 76),
('craft_beer', 'Specialty and artisanal beer selection', 'food_beverage', ARRAY['beer', 'quality'], 77),
('specialty_coffee', 'High-quality, expertly prepared coffee', 'food_beverage', ARRAY['coffee', 'quality'], 78),
('fresh_ingredients', 'Uses fresh, quality ingredients', 'food_beverage', ARRAY['quality', 'healthy'], 79),
('homemade', 'Made in-house from scratch', 'food_beverage', ARRAY['fresh', 'quality'], 80),
('international_cuisine', 'Food from various world cuisines', 'food_beverage', ARRAY['variety', 'cultural'], 81),
('local_cuisine', 'Traditional regional food specialties', 'food_beverage', ARRAY['traditional', 'cultural'], 82),
('fast_service', 'Quick service and short wait times', 'service_amenities', ARRAY['convenient', 'efficient'], 83),
('attentive_staff', 'Helpful and responsive service', 'service_amenities', ARRAY['service', 'quality'], 84),
('self_service', 'Customer self-service options', 'service_amenities', ARRAY['casual', 'efficient'], 85),
('group_friendly', 'Accommodates large groups well', 'service_amenities', ARRAY['social', 'spacious'], 86),
('date_spot', 'Good location for romantic dates', 'activities', ARRAY['romantic', 'intimate'], 87),
('work_friendly', 'Suitable for working and productivity', 'activities', ARRAY['wifi', 'quiet'], 88),
('student_hangout', 'Popular with students', 'activities', ARRAY['affordable', 'study'], 89),
('tourist_friendly', 'Welcomes and caters to tourists', 'service_amenities', ARRAY['helpful', 'inclusive'], 90),
('locals_favorite', 'Popular among local residents', 'activities', ARRAY['authentic', 'community'], 91),
('hidden_gem', 'Lesser-known but excellent place', 'location_accessibility', ARRAY['unique', 'special'], 92),
('popular_spot', 'Well-known and frequently visited', 'location_accessibility', ARRAY['busy', 'recommended'], 93),
('scenic_view', 'Beautiful views and surroundings', 'location_accessibility', ARRAY['beautiful', 'relaxing'], 94),
('central_location', 'Conveniently located in city center', 'location_accessibility', ARRAY['convenient', 'accessible'], 95),
('neighborhood_gem', 'Great local neighborhood establishment', 'location_accessibility', ARRAY['local', 'community'], 96),
('instagram_worthy', 'Photogenic and social media friendly', 'atmosphere', ARRAY['trendy', 'beautiful'], 97),
('authentic', 'Genuine and original character', 'atmosphere', ARRAY['traditional', 'real'], 98),
('unique', 'One-of-a-kind and distinctive', 'atmosphere', ARRAY['special', 'memorable'], 99);

-- Insert some key keyword relationships for vector similarity
INSERT INTO keyword_relationships (keyword1_id, keyword2_id, similarity_score, relationship_type) VALUES
-- Atmosphere relationships
(1, 8, 0.8, 'semantic'),  -- cozy <-> intimate
(1, 4, 0.7, 'semantic'),  -- cozy <-> quiet  
(2, 56, 0.9, 'semantic'), -- modern <-> trendy
(3, 57, 0.8, 'semantic'), -- vintage <-> traditional
(4, 37, 0.8, 'contextual'), -- quiet <-> study
(5, 38, 0.7, 'contextual'), -- lively <-> socializing
(6, 8, 0.9, 'semantic'), -- romantic <-> intimate

-- Food & Beverage relationships  
(11, 79, 0.8, 'contextual'), -- coffee <-> specialty_coffee
(12, 19, 0.6, 'contextual'), -- tea <-> organic
(13, 14, 0.7, 'category'), -- dessert <-> pastry
(16, 78, 0.8, 'contextual'), -- cocktail <-> craft_beer
(19, 75, 0.8, 'contextual'), -- organic <-> local_ingredients

-- Service relationships
(21, 22, 0.6, 'category'), -- wifi <-> parking
(26, 68, 0.7, 'contextual'), -- pet_friendly <-> family_friendly
(27, 69, 0.8, 'contextual'), -- wheelchair_accessible <-> family_friendly

-- Activity relationships  
(33, 34, 0.7, 'contextual'), -- board_games <-> workshops
(37, 4, 0.8, 'contextual'), -- study <-> quiet
(37, 89, 0.9, 'contextual'), -- socializing <-> student_hangout
(36, 46, 0.7, 'contextual'); -- meetings <-> business_district

-- Add function to calculate keyword vector from selected keywords
CREATE OR REPLACE FUNCTION calculate_keyword_vector(selected_keywords JSONB)
RETURNS vector(100) AS $$
DECLARE
    result_vector float8[100];
    keyword_obj JSON;
    keyword_text TEXT;
    confidence_score float8;
    vector_pos INTEGER;
BEGIN
    -- Initialize vector with zeros
    FOR i IN 1..100 LOOP
        result_vector[i] := 0.0;
    END LOOP;
    
    -- Process each selected keyword
    FOR keyword_obj IN SELECT value FROM jsonb_array_elements(selected_keywords)
    LOOP
        keyword_text := keyword_obj->>'keyword';
        confidence_score := (keyword_obj->>'confidence_score')::float8;
        
        -- Get vector position for this keyword
        SELECT vector_position INTO vector_pos 
        FROM keyword_catalog 
        WHERE keyword = keyword_text;
        
        -- Set the confidence score at the appropriate vector position
        IF vector_pos IS NOT NULL AND vector_pos >= 0 AND vector_pos < 100 THEN
            result_vector[vector_pos + 1] := confidence_score; -- Arrays are 1-indexed in PostgreSQL
        END IF;
    END LOOP;
    
    -- Return as pgvector type
    RETURN result_vector::vector(100);
END;
$$ LANGUAGE plpgsql;
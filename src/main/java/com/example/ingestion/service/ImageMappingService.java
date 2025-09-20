package com.example.ingestion.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for mapping place categories to default image paths
 * Based on the requirements for default image mapping
 */
@Service
public class ImageMappingService {

    private static final Logger logger = LoggerFactory.getLogger(ImageMappingService.class);

    // Default image mappings as specified in requirements
    private static final Map<String, String> CATEGORY_IMAGE_MAPPING = new ConcurrentHashMap<>();

    static {
        // Initialize category to image mappings
        CATEGORY_IMAGE_MAPPING.put("cafe", "/cafe.jpg");
        CATEGORY_IMAGE_MAPPING.put("palace", "/palace.jpg");
        CATEGORY_IMAGE_MAPPING.put("bar", "/bar.jpg");
        CATEGORY_IMAGE_MAPPING.put("korean-restaurant", "/korean-restaurant.jpg");
        CATEGORY_IMAGE_MAPPING.put("western-restaurant", "/western-restaurant.jpg");
        CATEGORY_IMAGE_MAPPING.put("chinese-restaurant", "/chinese-restaurant.jpg");
        CATEGORY_IMAGE_MAPPING.put("museum", "/museum.jpg");
        CATEGORY_IMAGE_MAPPING.put("theme-park", "/theme-park.jpg");
        CATEGORY_IMAGE_MAPPING.put("art-gallery", "/art-gallery.jpg");
        CATEGORY_IMAGE_MAPPING.put("theater", "/theater.jpg");
        CATEGORY_IMAGE_MAPPING.put("shopping-mall", "/shopping-mall.jpg");
        CATEGORY_IMAGE_MAPPING.put("park", "/park.jpg");
        CATEGORY_IMAGE_MAPPING.put("library", "/library.jpg");
        CATEGORY_IMAGE_MAPPING.put("spa", "/spa.jpg");
        CATEGORY_IMAGE_MAPPING.put("bakery", "/bakery.jpg");
        CATEGORY_IMAGE_MAPPING.put("fast-food", "/fast-food.jpg");
        CATEGORY_IMAGE_MAPPING.put("japanese-restaurant", "/japanese-restaurant.jpg");
        CATEGORY_IMAGE_MAPPING.put("seafood-restaurant", "/seafood-restaurant.jpg");
        CATEGORY_IMAGE_MAPPING.put("experience-space", "/experience-space.jpg");

        // Additional Korean category mappings
        CATEGORY_IMAGE_MAPPING.put("카페", "/cafe.jpg");
        CATEGORY_IMAGE_MAPPING.put("궁궐", "/palace.jpg");
        CATEGORY_IMAGE_MAPPING.put("술집", "/bar.jpg");
        CATEGORY_IMAGE_MAPPING.put("한식", "/korean-restaurant.jpg");
        CATEGORY_IMAGE_MAPPING.put("양식", "/western-restaurant.jpg");
        CATEGORY_IMAGE_MAPPING.put("중식", "/chinese-restaurant.jpg");
        CATEGORY_IMAGE_MAPPING.put("박물관", "/museum.jpg");
        CATEGORY_IMAGE_MAPPING.put("테마파크", "/theme-park.jpg");
        CATEGORY_IMAGE_MAPPING.put("미술관", "/art-gallery.jpg");
        CATEGORY_IMAGE_MAPPING.put("극장", "/theater.jpg");
        CATEGORY_IMAGE_MAPPING.put("쇼핑몰", "/shopping-mall.jpg");
        CATEGORY_IMAGE_MAPPING.put("공원", "/park.jpg");
        CATEGORY_IMAGE_MAPPING.put("도서관", "/library.jpg");
        CATEGORY_IMAGE_MAPPING.put("스파", "/spa.jpg");
        CATEGORY_IMAGE_MAPPING.put("베이커리", "/bakery.jpg");
        CATEGORY_IMAGE_MAPPING.put("패스트푸드", "/fast-food.jpg");
        CATEGORY_IMAGE_MAPPING.put("일식", "/japanese-restaurant.jpg");
        CATEGORY_IMAGE_MAPPING.put("해산물", "/seafood-restaurant.jpg");
        CATEGORY_IMAGE_MAPPING.put("체험공간", "/experience-space.jpg");

        // Common category mappings
        CATEGORY_IMAGE_MAPPING.put("restaurant", "/korean-restaurant.jpg");
        CATEGORY_IMAGE_MAPPING.put("food", "/korean-restaurant.jpg");
        CATEGORY_IMAGE_MAPPING.put("meal_takeaway", "/fast-food.jpg");
        CATEGORY_IMAGE_MAPPING.put("lodging", "/experience-space.jpg");
        CATEGORY_IMAGE_MAPPING.put("tourist_attraction", "/theme-park.jpg");
        CATEGORY_IMAGE_MAPPING.put("amusement_park", "/theme-park.jpg");
        CATEGORY_IMAGE_MAPPING.put("establishment", "/experience-space.jpg");
        CATEGORY_IMAGE_MAPPING.put("point_of_interest", "/experience-space.jpg");

        // Default fallback
        CATEGORY_IMAGE_MAPPING.put("default", "/default.jpg");
    }

    private static final String DEFAULT_IMAGE_PATH = "/default.jpg";

    /**
     * Get image path for a given category
     * @param category The place category
     * @return The mapped image path or default if not found
     */
    public String getImagePath(String category) {
        if (category == null || category.trim().isEmpty()) {
            logger.debug("Empty category provided, returning default image");
            return DEFAULT_IMAGE_PATH;
        }

        try {
            // First try exact match
            String slugifiedCategory = slugify(category);
            String imagePath = CATEGORY_IMAGE_MAPPING.get(slugifiedCategory);

            if (imagePath != null) {
                logger.debug("Found exact image mapping for category '{}': {}", category, imagePath);
                return imagePath;
            }

            // Try partial matches for complex categories
            imagePath = findPartialMatch(slugifiedCategory);
            if (imagePath != null) {
                logger.debug("Found partial image mapping for category '{}': {}", category, imagePath);
                return imagePath;
            }

            // Try Korean keyword matching
            imagePath = findKoreanKeywordMatch(category);
            if (imagePath != null) {
                logger.debug("Found Korean keyword mapping for category '{}': {}", category, imagePath);
                return imagePath;
            }

            logger.debug("No image mapping found for category '{}', using default", category);
            return DEFAULT_IMAGE_PATH;

        } catch (Exception e) {
            logger.warn("Error mapping category '{}' to image: {}", category, e.getMessage());
            return DEFAULT_IMAGE_PATH;
        }
    }

    /**
     * Convert category to slug format
     * @param category Original category
     * @return Slugified category
     */
    private String slugify(String category) {
        if (category == null) {
            return "";
        }

        return category.toLowerCase()
                .trim()
                .replaceAll("[^a-z0-9가-힣\\s-]", "") // Keep alphanumeric, Korean, spaces, hyphens
                .replaceAll("\\s+", "-") // Replace spaces with hyphens
                .replaceAll("-+", "-") // Remove multiple consecutive hyphens
                .replaceAll("^-|-$", ""); // Remove leading/trailing hyphens
    }

    /**
     * Find partial match for complex category names
     * @param slugifiedCategory Slugified category
     * @return Matched image path or null
     */
    private String findPartialMatch(String slugifiedCategory) {
        // Check for restaurant types
        if (slugifiedCategory.contains("restaurant") || slugifiedCategory.contains("레스토랑") ||
            slugifiedCategory.contains("음식점") || slugifiedCategory.contains("식당")) {

            if (slugifiedCategory.contains("korean") || slugifiedCategory.contains("한식")) {
                return "/korean-restaurant.jpg";
            }
            if (slugifiedCategory.contains("chinese") || slugifiedCategory.contains("중식")) {
                return "/chinese-restaurant.jpg";
            }
            if (slugifiedCategory.contains("japanese") || slugifiedCategory.contains("일식")) {
                return "/japanese-restaurant.jpg";
            }
            if (slugifiedCategory.contains("western") || slugifiedCategory.contains("양식")) {
                return "/western-restaurant.jpg";
            }
            if (slugifiedCategory.contains("seafood") || slugifiedCategory.contains("해산물")) {
                return "/seafood-restaurant.jpg";
            }
            return "/korean-restaurant.jpg"; // Default restaurant
        }

        // Check for cafe/coffee
        if (slugifiedCategory.contains("cafe") || slugifiedCategory.contains("coffee") ||
            slugifiedCategory.contains("카페") || slugifiedCategory.contains("커피")) {
            return "/cafe.jpg";
        }

        // Check for museum/gallery
        if (slugifiedCategory.contains("museum") || slugifiedCategory.contains("박물관")) {
            return "/museum.jpg";
        }
        if (slugifiedCategory.contains("gallery") || slugifiedCategory.contains("미술관")) {
            return "/art-gallery.jpg";
        }

        // Check for parks
        if (slugifiedCategory.contains("park") || slugifiedCategory.contains("공원")) {
            if (slugifiedCategory.contains("theme") || slugifiedCategory.contains("amusement") ||
                slugifiedCategory.contains("테마")) {
                return "/theme-park.jpg";
            }
            return "/park.jpg";
        }

        // Check for entertainment
        if (slugifiedCategory.contains("theater") || slugifiedCategory.contains("cinema") ||
            slugifiedCategory.contains("극장") || slugifiedCategory.contains("영화관")) {
            return "/theater.jpg";
        }

        return null;
    }

    /**
     * Find Korean keyword matches
     * @param category Original category
     * @return Matched image path or null
     */
    private String findKoreanKeywordMatch(String category) {
        String lowerCategory = category.toLowerCase();

        // Korean food keywords
        if (lowerCategory.contains("맛집") || lowerCategory.contains("음식")) {
            return "/korean-restaurant.jpg";
        }

        // Korean place keywords
        if (lowerCategory.contains("관광지") || lowerCategory.contains("명소")) {
            return "/theme-park.jpg";
        }

        if (lowerCategory.contains("문화") || lowerCategory.contains("예술")) {
            return "/art-gallery.jpg";
        }

        if (lowerCategory.contains("쇼핑")) {
            return "/shopping-mall.jpg";
        }

        if (lowerCategory.contains("휴식") || lowerCategory.contains("힐링")) {
            return "/spa.jpg";
        }

        return null;
    }

    /**
     * Get all available category mappings
     * @return Map of category to image path mappings
     */
    public Map<String, String> getAllMappings() {
        return Map.copyOf(CATEGORY_IMAGE_MAPPING);
    }

    /**
     * Add custom mapping (for dynamic updates)
     * @param category Category to map
     * @param imagePath Image path
     */
    public void addMapping(String category, String imagePath) {
        if (category != null && imagePath != null) {
            String slugifiedCategory = slugify(category);
            CATEGORY_IMAGE_MAPPING.put(slugifiedCategory, imagePath);
            logger.info("Added custom image mapping: {} -> {}", slugifiedCategory, imagePath);
        }
    }

    /**
     * Get mapping statistics
     * @return Statistics about the mapping service
     */
    public MappingStats getStats() {
        return new MappingStats(
            CATEGORY_IMAGE_MAPPING.size(),
            DEFAULT_IMAGE_PATH,
            CATEGORY_IMAGE_MAPPING.keySet().size()
        );
    }

    /**
     * Statistics class for mapping service
     */
    public static class MappingStats {
        private final int totalMappings;
        private final String defaultImagePath;
        private final int uniqueCategories;

        public MappingStats(int totalMappings, String defaultImagePath, int uniqueCategories) {
            this.totalMappings = totalMappings;
            this.defaultImagePath = defaultImagePath;
            this.uniqueCategories = uniqueCategories;
        }

        public int getTotalMappings() { return totalMappings; }
        public String getDefaultImagePath() { return defaultImagePath; }
        public int getUniqueCategories() { return uniqueCategories; }

        @Override
        public String toString() {
            return String.format("MappingStats{totalMappings=%d, uniqueCategories=%d, defaultPath='%s'}",
                               totalMappings, uniqueCategories, defaultImagePath);
        }
    }
}
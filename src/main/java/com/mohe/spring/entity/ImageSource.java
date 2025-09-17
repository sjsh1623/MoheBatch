package com.mohe.spring.entity;

public enum ImageSource {
    GOOGLE_IMAGES,     // Google Images API
    GOOGLE_PLACES,     // Google Places Photos API
    NAVER,             // Naver API
    MANUAL_UPLOAD,     // Admin uploaded
    WEB_SCRAPING,      // Web scraped (if allowed)
    AI_GENERATED,      // AI generated (OpenAI DALL-E, etc.)
    PENDING            // Placeholder record, waiting for batch processing
}
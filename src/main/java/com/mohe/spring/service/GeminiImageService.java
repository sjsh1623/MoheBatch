package com.mohe.spring.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
public class GeminiImageService {
    
    private static final Logger logger = LoggerFactory.getLogger(GeminiImageService.class);
    
    @Value("${app.gemini.api-key:}")
    private String geminiApiKey;
    
    @Value("${app.gemini.base-url:https://generativelanguage.googleapis.com/v1beta}")
    private String geminiBaseUrl;
    
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    
    public GeminiImageService(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }
    
    /**
     * Generate an image for a place using Gemini API
     * 
     * @param placeName Name of the place
     * @param placeDescription Description of the place
     * @param placeCategory Category of the place
     * @return Generated image URL or null if generation failed
     */
    public String generatePlaceImage(String placeName, String placeDescription, String placeCategory) {
        try {
            // Create a descriptive prompt for the place
            String imagePrompt = createImagePrompt(placeName, placeDescription, placeCategory);
            
            logger.info("Generating image for place: {} with prompt: {}", placeName, imagePrompt);
            
            // Call Gemini API to generate image
            String imageUrl = callGeminiImageApi(imagePrompt);
            
            if (imageUrl != null) {
                logger.info("Successfully generated image for place: {}", placeName);
                return imageUrl;
            } else {
                logger.warn("Failed to generate image for place: {}", placeName);
                return null;
            }
            
        } catch (Exception e) {
            logger.error("Error generating image for place: {}", placeName, e);
            return null;
        }
    }
    
    /**
     * Create a descriptive image prompt for the place
     */
    private String createImagePrompt(String placeName, String placeDescription, String placeCategory) {
        StringBuilder prompt = new StringBuilder();
        
        prompt.append("Create a high-quality, photorealistic image of ");
        prompt.append(placeName);
        
        if (placeCategory != null && !placeCategory.isEmpty()) {
            prompt.append(", which is a ").append(placeCategory);
        }
        
        if (placeDescription != null && !placeDescription.isEmpty()) {
            prompt.append(". ").append(placeDescription);
        }
        
        prompt.append(". The image should be suitable for a place recommendation app, ");
        prompt.append("showing the exterior or interior in an inviting way that would attract visitors. ");
        prompt.append("Use natural lighting and vibrant colors. The image should be 1024x1024 pixels.");
        
        return prompt.toString();
    }
    
    /**
     * Call Gemini API to generate image
     * Note: This is a placeholder implementation. The actual Gemini API for image generation
     * might use different endpoints or require different authentication methods.
     */
    private String callGeminiImageApi(String prompt) {
        try {
            // Check if API key is configured
            if (geminiApiKey == null || geminiApiKey.isEmpty()) {
                logger.warn("Gemini API key not configured. Please set app.gemini.api-key in environment variables or application.yml");
                return generateMockImageUrl(prompt);
            }

            logger.info("Attempting to generate image with Gemini API for prompt: {}", prompt);

            // For demonstration purposes, generate a mock image URL
            // In a real implementation, you would call the actual Gemini image generation API
            String mockImageUrl = generateMockImageUrl(prompt);

            // Log the generated URL for debugging
            logger.info("Generated mock image URL: {}", mockImageUrl);

            return mockImageUrl;

        } catch (Exception e) {
            logger.error("Error calling Gemini API", e);
            return null;
        }
    }
    
    /**
     * Generate a mock image URL for demonstration purposes
     * In a real implementation, this would be replaced with actual API calls
     */
    private String generateMockImageUrl(String prompt) {
        // Create a hash-based URL for consistent results
        String hash = String.valueOf(Math.abs(prompt.hashCode()));
        return "https://picsum.photos/1024/1024?random=" + hash;
    }
}
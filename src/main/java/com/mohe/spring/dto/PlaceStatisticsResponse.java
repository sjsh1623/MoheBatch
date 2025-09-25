package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class PlaceStatisticsResponse {
    
    @JsonProperty("total_places")
    private Long totalPlaces;
    
    @JsonProperty("places_with_images")
    private Long placesWithImages;
    
    @JsonProperty("places_with_mbti_descriptions")
    private Long placesWithMbtiDescriptions;
    
    @JsonProperty("places_with_keywords")
    private Long placesWithKeywords;
    
    @JsonProperty("places_with_similarities")
    private Long placesWithSimilarities;
    
    @JsonProperty("average_rating")
    private Double averageRating;
    
    @JsonProperty("category_breakdown")
    private Map<String, Long> categoryBreakdown;
    
    @JsonProperty("mbti_coverage")
    private Map<String, Long> mbtiCoverage;
    
    @JsonProperty("image_statistics")
    private ImageStatistics imageStatistics;
    
    @JsonProperty("keyword_statistics")
    private KeywordStatistics keywordStatistics;
    
    // Default constructor
    public PlaceStatisticsResponse() {}
    
    // Constructor with fields
    public PlaceStatisticsResponse(Long totalPlaces, Long placesWithImages,
                                  Long placesWithMbtiDescriptions, Long placesWithKeywords,
                                  Long placesWithSimilarities, Double averageRating,
                                  Map<String, Long> categoryBreakdown, Map<String, Long> mbtiCoverage,
                                  ImageStatistics imageStatistics, KeywordStatistics keywordStatistics) {
        this.totalPlaces = totalPlaces;
        this.placesWithImages = placesWithImages;
        this.placesWithMbtiDescriptions = placesWithMbtiDescriptions;
        this.placesWithKeywords = placesWithKeywords;
        this.placesWithSimilarities = placesWithSimilarities;
        this.averageRating = averageRating;
        this.categoryBreakdown = categoryBreakdown;
        this.mbtiCoverage = mbtiCoverage;
        this.imageStatistics = imageStatistics;
        this.keywordStatistics = keywordStatistics;
    }
    
    // Nested classes for statistics
    public static class ImageStatistics {
        @JsonProperty("total_images")
        private Long totalImages;
        
        @JsonProperty("average_images_per_place")
        private Double averageImagesPerPlace;
        
        @JsonProperty("images_by_type")
        private Map<String, Long> imagesByType;
        
        // Constructor and getters/setters
        public ImageStatistics() {}
        public ImageStatistics(Long totalImages, Double averageImagesPerPlace, Map<String, Long> imagesByType) {
            this.totalImages = totalImages;
            this.averageImagesPerPlace = averageImagesPerPlace;
            this.imagesByType = imagesByType;
        }
        
        public Long getTotalImages() { return totalImages; }
        public void setTotalImages(Long totalImages) { this.totalImages = totalImages; }
        public Double getAverageImagesPerPlace() { return averageImagesPerPlace; }
        public void setAverageImagesPerPlace(Double averageImagesPerPlace) { this.averageImagesPerPlace = averageImagesPerPlace; }
        public Map<String, Long> getImagesByType() { return imagesByType; }
        public void setImagesByType(Map<String, Long> imagesByType) { this.imagesByType = imagesByType; }
    }
    
    public static class KeywordStatistics {
        @JsonProperty("total_keywords")
        private Long totalKeywords;
        
        @JsonProperty("average_keywords_per_place")
        private Double averageKeywordsPerPlace;
        
        @JsonProperty("most_common_keywords")
        private Map<String, Long> mostCommonKeywords;
        
        // Constructor and getters/setters
        public KeywordStatistics() {}
        public KeywordStatistics(Long totalKeywords, Double averageKeywordsPerPlace, Map<String, Long> mostCommonKeywords) {
            this.totalKeywords = totalKeywords;
            this.averageKeywordsPerPlace = averageKeywordsPerPlace;
            this.mostCommonKeywords = mostCommonKeywords;
        }
        
        public Long getTotalKeywords() { return totalKeywords; }
        public void setTotalKeywords(Long totalKeywords) { this.totalKeywords = totalKeywords; }
        public Double getAverageKeywordsPerPlace() { return averageKeywordsPerPlace; }
        public void setAverageKeywordsPerPlace(Double averageKeywordsPerPlace) { this.averageKeywordsPerPlace = averageKeywordsPerPlace; }
        public Map<String, Long> getMostCommonKeywords() { return mostCommonKeywords; }
        public void setMostCommonKeywords(Map<String, Long> mostCommonKeywords) { this.mostCommonKeywords = mostCommonKeywords; }
    }
    
    // Getters and setters
    public Long getTotalPlaces() {
        return totalPlaces;
    }
    
    public void setTotalPlaces(Long totalPlaces) {
        this.totalPlaces = totalPlaces;
    }
    
    public Long getPlacesWithImages() {
        return placesWithImages;
    }
    
    public void setPlacesWithImages(Long placesWithImages) {
        this.placesWithImages = placesWithImages;
    }
    
    public Long getPlacesWithMbtiDescriptions() {
        return placesWithMbtiDescriptions;
    }
    
    public void setPlacesWithMbtiDescriptions(Long placesWithMbtiDescriptions) {
        this.placesWithMbtiDescriptions = placesWithMbtiDescriptions;
    }
    
    public Long getPlacesWithKeywords() {
        return placesWithKeywords;
    }
    
    public void setPlacesWithKeywords(Long placesWithKeywords) {
        this.placesWithKeywords = placesWithKeywords;
    }
    
    public Long getPlacesWithSimilarities() {
        return placesWithSimilarities;
    }
    
    public void setPlacesWithSimilarities(Long placesWithSimilarities) {
        this.placesWithSimilarities = placesWithSimilarities;
    }
    
    public Double getAverageRating() {
        return averageRating;
    }
    
    public void setAverageRating(Double averageRating) {
        this.averageRating = averageRating;
    }
    
    public Map<String, Long> getCategoryBreakdown() {
        return categoryBreakdown;
    }
    
    public void setCategoryBreakdown(Map<String, Long> categoryBreakdown) {
        this.categoryBreakdown = categoryBreakdown;
    }
    
    public Map<String, Long> getMbtiCoverage() {
        return mbtiCoverage;
    }
    
    public void setMbtiCoverage(Map<String, Long> mbtiCoverage) {
        this.mbtiCoverage = mbtiCoverage;
    }
    
    public ImageStatistics getImageStatistics() {
        return imageStatistics;
    }
    
    public void setImageStatistics(ImageStatistics imageStatistics) {
        this.imageStatistics = imageStatistics;
    }
    
    public KeywordStatistics getKeywordStatistics() {
        return keywordStatistics;
    }
    
    public void setKeywordStatistics(KeywordStatistics keywordStatistics) {
        this.keywordStatistics = keywordStatistics;
    }
}
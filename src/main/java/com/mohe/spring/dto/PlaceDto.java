package com.mohe.spring.dto;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class PlaceDto {

    // Place List and Recommendations
    public record PlaceListResponse(
        List<PlaceCardData> places,
        PaginationData pagination
    ) {}

    public record PlaceRecommendationsResponse(
        List<PlaceRecommendationData> recommendations,
        int totalCount
    ) {}

    public static class PlaceCardData {
        private final String id;
        private final String title;
        private final Double rating;
        private final Integer reviewCount;
        private final String location;
        private final String image;
        private final List<PlaceImageData> images;
        private final boolean isBookmarked;

        public PlaceCardData(String id, String title, Double rating, Integer reviewCount, 
                           String location, String image, List<PlaceImageData> images, 
                           boolean isBookmarked) {
            this.id = id;
            this.title = title;
            this.rating = rating;
            this.reviewCount = reviewCount;
            this.location = location;
            this.image = image;
            this.images = images != null ? images : List.of();
            this.isBookmarked = isBookmarked;
        }

        // Helper method to get formatted rating with one decimal place
        public String getFormattedRating() {
            return rating != null ? String.format("%.1f", rating) : null;
        }

        // Getters
        public String getId() { return id; }
        public String getTitle() { return title; }
        public Double getRating() { return rating; }
        public Integer getReviewCount() { return reviewCount; }
        public String getLocation() { return location; }
        public String getImage() { return image; }
        public List<PlaceImageData> getImages() { return images; }
        public boolean isBookmarked() { return isBookmarked; }
    }

    public static class PlaceRecommendationData {
        private final String id;
        private final String title;
        private final Double rating;
        private final int reviewCount;
        private final String location;
        private final String image;
        private final List<PlaceImageData> images;
        private final List<String> tags;
        private final String description;
        private final TransportationInfo transportation;
        private final boolean isBookmarked;
        private final String recommendationReason;

        public PlaceRecommendationData(String id, String title, Double rating, int reviewCount,
                                     String location, String image, List<PlaceImageData> images,
                                     List<String> tags, String description, TransportationInfo transportation,
                                     boolean isBookmarked, String recommendationReason) {
            this.id = id;
            this.title = title;
            this.rating = rating;
            this.reviewCount = reviewCount;
            this.location = location;
            this.image = image;
            this.images = images != null ? images : List.of();
            this.tags = tags;
            this.description = description;
            this.transportation = transportation;
            this.isBookmarked = isBookmarked;
            this.recommendationReason = recommendationReason;
        }

        // Helper method to get formatted rating with one decimal place
        public String getFormattedRating() {
            return rating != null ? String.format("%.1f", rating) : null;
        }

        // Getters
        public String getId() { return id; }
        public String getTitle() { return title; }
        public Double getRating() { return rating; }
        public int getReviewCount() { return reviewCount; }
        public String getLocation() { return location; }
        public String getImage() { return image; }
        public List<PlaceImageData> getImages() { return images; }
        public List<String> getTags() { return tags; }
        public String getDescription() { return description; }
        public TransportationInfo getTransportation() { return transportation; }
        public boolean isBookmarked() { return isBookmarked; }
        public String getRecommendationReason() { return recommendationReason; }
    }

    public record TransportationInfo(
        String car,
        String bus
    ) {}

    public record PaginationData(
        int currentPage,
        int totalPages,
        int totalCount
    ) {}

    // Place Detail
    public record PlaceDetailResponse(
        PlaceDetailData place
    ) {}

    // Helper functions to convert between BigDecimal and Double
    public static Double toFormattedDouble(BigDecimal value) {
        return value != null ? value.setScale(1, RoundingMode.HALF_UP).doubleValue() : null;
    }

    public static BigDecimal toBigDecimal(Double value) {
        return value != null ? BigDecimal.valueOf(value).setScale(1, RoundingMode.HALF_UP) : BigDecimal.ZERO;
    }

    public static class PlaceDetailData {
        private final String id;
        private final String title;
        private final List<String> tags;
        private final String location;
        private final String address;
        private final Double rating;
        private final int reviewCount;
        private final String description;
        private final List<PlaceImageData> images;
        private final List<String> gallery;
        private final int additionalImageCount;
        private final TransportationInfo transportation;
        private final String operatingHours;
        private final List<String> amenities;
        private final boolean isBookmarked;

        public PlaceDetailData(String id, String title, List<String> tags, String location, String address,
                             Double rating, int reviewCount, String description, List<PlaceImageData> images,
                             List<String> gallery, int additionalImageCount, TransportationInfo transportation,
                             String operatingHours, List<String> amenities, boolean isBookmarked) {
            this.id = id;
            this.title = title;
            this.tags = tags;
            this.location = location;
            this.address = address;
            this.rating = rating;
            this.reviewCount = reviewCount;
            this.description = description;
            this.images = images;
            this.gallery = gallery;
            this.additionalImageCount = additionalImageCount;
            this.transportation = transportation;
            this.operatingHours = operatingHours;
            this.amenities = amenities;
            this.isBookmarked = isBookmarked;
        }

        // Helper method to get formatted rating with one decimal place
        public String getFormattedRating() {
            return rating != null ? String.format("%.1f", rating) : null;
        }

        // Getters
        public String getId() { return id; }
        public String getTitle() { return title; }
        public List<String> getTags() { return tags; }
        public String getLocation() { return location; }
        public String getAddress() { return address; }
        public Double getRating() { return rating; }
        public int getReviewCount() { return reviewCount; }
        public String getDescription() { return description; }
        public List<PlaceImageData> getImages() { return images; }
        public List<String> getGallery() { return gallery; }
        public int getAdditionalImageCount() { return additionalImageCount; }
        public TransportationInfo getTransportation() { return transportation; }
        public String getOperatingHours() { return operatingHours; }
        public List<String> getAmenities() { return amenities; }
        public boolean isBookmarked() { return isBookmarked; }
    }

    // Search
    public record PlaceSearchResponse(
        List<PlaceSearchResult> searchResults,
        SearchContext searchContext
    ) {}

    public static class PlaceSearchResult {
        private final String id;
        private final String name;
        private final String hours;
        private final String location;
        private final Double rating;
        private final String carTime;
        private final String busTime;
        private final List<String> tags;
        private final String image;
        private final List<PlaceImageData> images;
        private final boolean isBookmarked;
        private final TagInfo weatherTag;
        private final TagInfo noiseTag;

        public PlaceSearchResult(String id, String name, String hours, String location, Double rating,
                               String carTime, String busTime, List<String> tags, String image,
                               List<PlaceImageData> images, boolean isBookmarked, TagInfo weatherTag, TagInfo noiseTag) {
            this.id = id;
            this.name = name;
            this.hours = hours;
            this.location = location;
            this.rating = rating;
            this.carTime = carTime;
            this.busTime = busTime;
            this.tags = tags;
            this.image = image;
            this.images = images != null ? images : List.of();
            this.isBookmarked = isBookmarked;
            this.weatherTag = weatherTag;
            this.noiseTag = noiseTag;
        }

        // Helper method to get formatted rating with one decimal place
        public String getFormattedRating() {
            return rating != null ? String.format("%.1f", rating) : null;
        }

        // Getters
        public String getId() { return id; }
        public String getName() { return name; }
        public String getHours() { return hours; }
        public String getLocation() { return location; }
        public Double getRating() { return rating; }
        public String getCarTime() { return carTime; }
        public String getBusTime() { return busTime; }
        public List<String> getTags() { return tags; }
        public String getImage() { return image; }
        public List<PlaceImageData> getImages() { return images; }
        public boolean isBookmarked() { return isBookmarked; }
        public TagInfo getWeatherTag() { return weatherTag; }
        public TagInfo getNoiseTag() { return noiseTag; }
    }

    public record TagInfo(
        String text,
        String color,
        String icon
    ) {}

    public record SearchContext(
        String weather,
        String time,
        String recommendation
    ) {}

    // New record for place images
    public record PlaceImageData(
        long id,
        String imageUrl,
        String imageType,
        boolean isPrimary,
        int displayOrder,
        String source,
        Integer width,
        Integer height,
        String altText,
        String caption
    ) {}

    // New record for "지금 이 시간의 장소" response
    public record CurrentTimeRecommendationsResponse(
        List<PlaceCardData> places,
        CurrentTimeContext context
    ) {}

    public record CurrentTimeContext(
        String timeOfDay, // "morning", "afternoon", "evening", "night"
        String weatherCondition, // "sunny", "rainy", "cloudy", etc.
        String recommendationMessage
    ) {}

    // Simple response for home page images
    public static class PlaceResponse {
        private final Long id;
        private final String name;
        private final String imageUrl;
        private final List<String> images;
        private final Double rating;
        private final String category;
        private Double distance = 0.0; // Default to 0 as per requirements

        public PlaceResponse(Long id, String name, String imageUrl, List<String> images, Double rating, String category) {
            this.id = id;
            this.name = name;
            this.imageUrl = imageUrl;
            this.images = images != null ? images : List.of();
            this.rating = rating;
            this.category = category;
        }
        
        // Backward compatibility constructor
        public PlaceResponse(Long id, String name, String imageUrl, Double rating, String category) {
            this(id, name, imageUrl, List.of(), rating, category);
        }

        // Getters and setters
        public Long getId() { return id; }
        public String getName() { return name; }
        public String getImageUrl() { return imageUrl; }
        public List<String> getImages() { return images; }
        public Double getRating() { return rating; }
        public String getCategory() { return category; }
        public Double getDistance() { return distance; }
        public void setDistance(Double distance) { this.distance = distance; }
    }
}
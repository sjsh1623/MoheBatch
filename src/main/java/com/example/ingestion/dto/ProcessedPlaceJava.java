package com.example.ingestion.dto;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Java equivalent of ProcessedPlace for mutable operations
 */
public class ProcessedPlaceJava {
    private String naverPlaceId;
    private String googlePlaceId;
    private String name;
    private String description;
    private String category;
    private String address;
    private String roadAddress;
    private BigDecimal latitude;
    private BigDecimal longitude;
    private String phone;
    private String websiteUrl;
    private Double rating;
    private Integer userRatingsTotal;
    private Integer priceLevel;
    private List<String> types;
    private String openingHours; // JSON string
    private List<String> gallery; // Image URLs
    private Map<String, Object> sourceFlags;
    private String naverRawData; // JSON string
    private String googleRawData; // JSON string
    private List<Double> keywordVector; // Embedding vector from Ollama
    private List<String> tags; // Place tags
    private List<String> amenities; // Place amenities
    private List<String> images; // Place images
    private Integer reviewCount; // Number of reviews

    public ProcessedPlaceJava() {
        this.sourceFlags = new HashMap<>();
        this.gallery = new ArrayList<>();
        this.keywordVector = new ArrayList<>();
        this.types = new ArrayList<>();
        this.tags = new ArrayList<>();
        this.amenities = new ArrayList<>();
        this.images = new ArrayList<>();
        this.reviewCount = 0;
    }

    // Getters and Setters
    public String getNaverPlaceId() { return naverPlaceId; }
    public void setNaverPlaceId(String naverPlaceId) { this.naverPlaceId = naverPlaceId; }

    public String getGooglePlaceId() { return googlePlaceId; }
    public void setGooglePlaceId(String googlePlaceId) { this.googlePlaceId = googlePlaceId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }

    public String getRoadAddress() { return roadAddress; }
    public void setRoadAddress(String roadAddress) { this.roadAddress = roadAddress; }

    public BigDecimal getLatitude() { return latitude; }
    public void setLatitude(BigDecimal latitude) { this.latitude = latitude; }

    public BigDecimal getLongitude() { return longitude; }
    public void setLongitude(BigDecimal longitude) { this.longitude = longitude; }

    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }

    public String getWebsiteUrl() { return websiteUrl; }
    public void setWebsiteUrl(String websiteUrl) { this.websiteUrl = websiteUrl; }

    public Double getRating() { return rating; }
    public void setRating(Double rating) { this.rating = rating; }

    public Integer getUserRatingsTotal() { return userRatingsTotal; }
    public void setUserRatingsTotal(Integer userRatingsTotal) { this.userRatingsTotal = userRatingsTotal; }

    public Integer getPriceLevel() { return priceLevel; }
    public void setPriceLevel(Integer priceLevel) { this.priceLevel = priceLevel; }

    public List<String> getTypes() { return types; }
    public void setTypes(List<String> types) { this.types = types; }

    public String getOpeningHours() { return openingHours; }
    public void setOpeningHours(String openingHours) { this.openingHours = openingHours; }

    public List<String> getGallery() { return gallery; }
    public void setGallery(List<String> gallery) { this.gallery = gallery; }

    public Map<String, Object> getSourceFlags() { return sourceFlags; }
    public void setSourceFlags(Map<String, Object> sourceFlags) { this.sourceFlags = sourceFlags; }

    public String getNaverRawData() { return naverRawData; }
    public void setNaverRawData(String naverRawData) { this.naverRawData = naverRawData; }

    public String getGoogleRawData() { return googleRawData; }
    public void setGoogleRawData(String googleRawData) { this.googleRawData = googleRawData; }

    public List<Double> getKeywordVector() { return keywordVector; }
    public void setKeywordVector(List<Double> keywordVector) { this.keywordVector = keywordVector; }

    public List<String> getTags() { return tags; }
    public void setTags(List<String> tags) { this.tags = tags; }

    public List<String> getAmenities() { return amenities; }
    public void setAmenities(List<String> amenities) { this.amenities = amenities; }

    public List<String> getImages() { return images; }
    public void setImages(List<String> images) { this.images = images; }

    public Integer getReviewCount() { return reviewCount; }
    public void setReviewCount(Integer reviewCount) { this.reviewCount = reviewCount; }
}
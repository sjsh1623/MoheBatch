package com.mohe.spring.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonType;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.Type;
import org.hibernate.type.SqlTypes;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "places")
public class Place {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private String name;
    
    @Column(nullable = false)
    private String title; // For backward compatibility
    
    private String address;
    
    @Column(name = "road_address")
    private String roadAddress;
    
    private String location;
    
    @Column(precision = 10, scale = 8)
    private BigDecimal latitude;
    
    @Column(precision = 11, scale = 8)
    private BigDecimal longitude;
    
    @Column(precision = 10, scale = 2)
    private BigDecimal altitude;
    
    private String category;
    private String description;
    
    @Column(name = "image_url")
    private String imageUrl;
    
    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(columnDefinition = "text[]")
    private List<String> images = new ArrayList<>();
    
    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(columnDefinition = "text[]")
    private List<String> gallery = new ArrayList<>();
    
    
    @Column(precision = 3, scale = 2)
    private BigDecimal rating = BigDecimal.ZERO;
    
    @Column(name = "review_count")
    private Integer reviewCount = 0;
    
    
    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(columnDefinition = "text[]")
    private List<String> amenities = new ArrayList<>();
    
    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(columnDefinition = "text[]")
    private List<String> tags = new ArrayList<>();
    
    
    private Integer popularity = 0;
    
    @Column(name = "created_at")
    private OffsetDateTime createdAt = OffsetDateTime.now();
    
    // New enhanced fields
    @Column(name = "naver_place_id", unique = true)
    private String naverPlaceId;
    
    @Column(name = "google_place_id", unique = true)
    private String googlePlaceId;
    
    private String phone;
    
    @Column(name = "website_url")
    private String websiteUrl;
    
    @Type(JsonType.class)
    @Column(name = "opening_hours", columnDefinition = "jsonb")
    private JsonNode openingHours;
    
    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "types", columnDefinition = "text[]")
    private List<String> types = new ArrayList<>();
    
    @Column(name = "user_ratings_total")
    private Integer userRatingsTotal;
    
    @Column(name = "price_level")
    private Short priceLevel;
    
    @Type(JsonType.class)
    @Column(name = "source_flags", columnDefinition = "jsonb")
    private JsonNode sourceFlags;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt = LocalDateTime.now();
    
    // Age tracking fields
    @Column(name = "opened_date")
    private LocalDate openedDate;
    
    @Column(name = "first_seen_at")
    private OffsetDateTime firstSeenAt = OffsetDateTime.now();
    
    @Column(name = "last_rating_check")
    private OffsetDateTime lastRatingCheck;
    
    @Column(name = "is_new_place")
    private Boolean isNewPlace = true;
    
    @Column(name = "should_recheck_rating")
    private Boolean shouldRecheckRating = false;
    
    @Column(name = "keyword_vector", columnDefinition = "text")
    private String keywordVector; // Stored as JSON array string format
    
    // Relationships
    @OneToMany(mappedBy = "place", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<Bookmark> bookmarks = new ArrayList<>();
    
    @OneToMany(mappedBy = "place", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<Prompt> prompts = new ArrayList<>();
    
    @OneToMany(mappedBy = "place", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<RecentView> recentViews = new ArrayList<>();
    
    // Default constructor for JPA
    public Place() {}
    
    // Constructor with required fields
    public Place(String name) {
        this.name = name;
        this.title = name; // For backward compatibility
    }
    
    // Helper methods
    public List<String> getTypesList() {
        return types;
    }
    
    public boolean isOlderThanSixMonths() {
        return firstSeenAt.isBefore(OffsetDateTime.now().minusMonths(6));
    }
    
    public boolean isRecentlyOpened() {
        return openedDate != null && openedDate.isAfter(LocalDate.now().minusMonths(6));
    }
    
    public boolean shouldBeRecommended() {
        // Recommend if rating >= 3.0 OR if it's a new place (< 6 months old)
        boolean hasGoodRating = rating.doubleValue() >= 3.0;
        boolean isNew = (isNewPlace != null && isNewPlace) || isRecentlyOpened();
        return hasGoodRating || isNew;
    }
    
    public boolean needsRatingRecheck() {
        return (shouldRecheckRating != null && shouldRecheckRating) && isOlderThanSixMonths();
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getTitle() {
        return title;
    }
    
    public void setTitle(String title) {
        this.title = title;
    }
    
    public String getAddress() {
        return address;
    }
    
    public void setAddress(String address) {
        this.address = address;
    }
    
    public String getRoadAddress() {
        return roadAddress;
    }
    
    public void setRoadAddress(String roadAddress) {
        this.roadAddress = roadAddress;
    }
    
    public String getLocation() {
        return location;
    }
    
    public void setLocation(String location) {
        this.location = location;
    }
    
    public BigDecimal getLatitude() {
        return latitude;
    }
    
    public void setLatitude(BigDecimal latitude) {
        this.latitude = latitude;
    }
    
    public BigDecimal getLongitude() {
        return longitude;
    }
    
    public void setLongitude(BigDecimal longitude) {
        this.longitude = longitude;
    }
    
    public BigDecimal getAltitude() {
        return altitude;
    }
    
    public void setAltitude(BigDecimal altitude) {
        this.altitude = altitude;
    }
    
    public String getCategory() {
        return category;
    }
    
    public void setCategory(String category) {
        this.category = category;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public String getImageUrl() {
        return imageUrl;
    }
    
    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }
    
    public List<String> getImages() {
        return images;
    }
    
    public void setImages(List<String> images) {
        this.images = images;
    }
    
    public List<String> getGallery() {
        return gallery;
    }
    
    public void setGallery(List<String> gallery) {
        this.gallery = gallery;
    }
    
    
    public BigDecimal getRating() {
        return rating;
    }
    
    public void setRating(BigDecimal rating) {
        this.rating = rating;
    }
    
    public Integer getReviewCount() {
        return reviewCount;
    }
    
    public void setReviewCount(Integer reviewCount) {
        this.reviewCount = reviewCount;
    }
    
    
    public List<String> getAmenities() {
        return amenities;
    }
    
    public void setAmenities(List<String> amenities) {
        this.amenities = amenities;
    }
    
    public List<String> getTags() {
        return tags;
    }
    
    public void setTags(List<String> tags) {
        this.tags = tags;
    }
    
    
    public Integer getPopularity() {
        return popularity;
    }
    
    public void setPopularity(Integer popularity) {
        this.popularity = popularity;
    }
    
    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public String getNaverPlaceId() {
        return naverPlaceId;
    }
    
    public void setNaverPlaceId(String naverPlaceId) {
        this.naverPlaceId = naverPlaceId;
    }
    
    public String getGooglePlaceId() {
        return googlePlaceId;
    }
    
    public void setGooglePlaceId(String googlePlaceId) {
        this.googlePlaceId = googlePlaceId;
    }
    
    public String getPhone() {
        return phone;
    }
    
    public void setPhone(String phone) {
        this.phone = phone;
    }
    
    public String getWebsiteUrl() {
        return websiteUrl;
    }
    
    public void setWebsiteUrl(String websiteUrl) {
        this.websiteUrl = websiteUrl;
    }
    
    public JsonNode getOpeningHours() {
        return openingHours;
    }
    
    public void setOpeningHours(JsonNode openingHours) {
        this.openingHours = openingHours;
    }
    
    public List<String> getTypes() {
        return types;
    }
    
    public void setTypes(List<String> types) {
        this.types = types;
    }
    
    public Integer getUserRatingsTotal() {
        return userRatingsTotal;
    }
    
    public void setUserRatingsTotal(Integer userRatingsTotal) {
        this.userRatingsTotal = userRatingsTotal;
    }
    
    public Short getPriceLevel() {
        return priceLevel;
    }
    
    public void setPriceLevel(Short priceLevel) {
        this.priceLevel = priceLevel;
    }
    
    public JsonNode getSourceFlags() {
        return sourceFlags;
    }
    
    public void setSourceFlags(JsonNode sourceFlags) {
        this.sourceFlags = sourceFlags;
    }
    
    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
    
    public LocalDate getOpenedDate() {
        return openedDate;
    }
    
    public void setOpenedDate(LocalDate openedDate) {
        this.openedDate = openedDate;
    }
    
    public OffsetDateTime getFirstSeenAt() {
        return firstSeenAt;
    }
    
    public void setFirstSeenAt(OffsetDateTime firstSeenAt) {
        this.firstSeenAt = firstSeenAt;
    }
    
    public OffsetDateTime getLastRatingCheck() {
        return lastRatingCheck;
    }
    
    public void setLastRatingCheck(OffsetDateTime lastRatingCheck) {
        this.lastRatingCheck = lastRatingCheck;
    }
    
    public Boolean getIsNewPlace() {
        return isNewPlace;
    }
    
    public void setIsNewPlace(Boolean isNewPlace) {
        this.isNewPlace = isNewPlace;
    }
    
    public Boolean getShouldRecheckRating() {
        return shouldRecheckRating;
    }
    
    public void setShouldRecheckRating(Boolean shouldRecheckRating) {
        this.shouldRecheckRating = shouldRecheckRating;
    }
    
    public String getKeywordVector() {
        return keywordVector;
    }
    
    public void setKeywordVector(String keywordVector) {
        this.keywordVector = keywordVector;
    }
    
    public List<Bookmark> getBookmarks() {
        return bookmarks;
    }
    
    public void setBookmarks(List<Bookmark> bookmarks) {
        this.bookmarks = bookmarks;
    }
    
    public List<Prompt> getPrompts() {
        return prompts;
    }
    
    public void setPrompts(List<Prompt> prompts) {
        this.prompts = prompts;
    }
    
    public List<RecentView> getRecentViews() {
        return recentViews;
    }
    
    public void setRecentViews(List<RecentView> recentViews) {
        this.recentViews = recentViews;
    }
}
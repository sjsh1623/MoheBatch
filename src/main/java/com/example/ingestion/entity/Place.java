package com.example.ingestion.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Place Entity - 기존 Spring 구조와 동일하게 매핑
 * MoheSpring의 places 테이블과 호환
 */
@Entity
@Table(name = "places")
public class Place {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "name", nullable = false)
    private String name;

    @Column(name = "address", columnDefinition = "TEXT")
    private String address;

    @Column(name = "road_address", columnDefinition = "TEXT")
    private String roadAddress;

    @Column(name = "latitude", precision = 10, scale = 8)
    private BigDecimal latitude;

    @Column(name = "longitude", precision = 11, scale = 8)
    private BigDecimal longitude;

    @Column(name = "category", length = 100)
    private String category;

    @Column(name = "description", columnDefinition = "TEXT")
    private String description;

    @Column(name = "image_url", columnDefinition = "TEXT")
    private String imageUrl;

    @Column(name = "gallery", columnDefinition = "TEXT[]")
    private String[] gallery;

    @Column(name = "rating", precision = 3, scale = 2)
    private BigDecimal rating = BigDecimal.ZERO;

    @Column(name = "review_count")
    private Integer reviewCount = 0;

    @Column(name = "amenities", columnDefinition = "TEXT[]")
    private String[] amenities;

    @Column(name = "tags", columnDefinition = "TEXT[]")
    private String[] tags;

    @Column(name = "popularity")
    private Integer popularity = 0;

    @Column(name = "naver_place_id")
    private String naverPlaceId;

    @Column(name = "google_place_id")
    private String googlePlaceId;

    @Column(name = "phone", length = 50)
    private String phone;

    @Column(name = "website_url", columnDefinition = "TEXT")
    private String websiteUrl;

    @Column(name = "opening_hours", columnDefinition = "JSONB")
    private String openingHours;

    @Column(name = "types", columnDefinition = "TEXT[]")
    private String[] types;

    @Column(name = "user_ratings_total")
    private Integer userRatingsTotal = 0;

    @Column(name = "price_level")
    private Integer priceLevel;

    @Column(name = "source_flags", length = 50)
    private String sourceFlags = "UNKNOWN";

    @Column(name = "opened_date")
    private LocalDate openedDate;

    @Column(name = "first_seen_at")
    private ZonedDateTime firstSeenAt;

    @Column(name = "last_rating_check")
    private ZonedDateTime lastRatingCheck;

    @Column(name = "is_new_place")
    private Boolean isNewPlace = false;

    @Column(name = "should_recheck_rating")
    private Boolean shouldRecheckRating = false;

    @Column(name = "keyword_vector", columnDefinition = "TEXT")
    private String keywordVector;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private ZonedDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private ZonedDateTime updatedAt;

    // Relationships
    @OneToMany(mappedBy = "place", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<PlaceImage> placeImages = new ArrayList<>();

    // Constructors
    public Place() {}

    public Place(String name, String address) {
        this.name = name;
        this.address = address;
        this.firstSeenAt = ZonedDateTime.now();
    }

    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }

    public String getRoadAddress() { return roadAddress; }
    public void setRoadAddress(String roadAddress) { this.roadAddress = roadAddress; }

    public BigDecimal getLatitude() { return latitude; }
    public void setLatitude(BigDecimal latitude) { this.latitude = latitude; }

    public BigDecimal getLongitude() { return longitude; }
    public void setLongitude(BigDecimal longitude) { this.longitude = longitude; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getImageUrl() { return imageUrl; }
    public void setImageUrl(String imageUrl) { this.imageUrl = imageUrl; }

    public String[] getGallery() { return gallery; }
    public void setGallery(String[] gallery) { this.gallery = gallery; }

    public BigDecimal getRating() { return rating; }
    public void setRating(BigDecimal rating) { this.rating = rating; }

    public Integer getReviewCount() { return reviewCount; }
    public void setReviewCount(Integer reviewCount) { this.reviewCount = reviewCount; }

    public String[] getAmenities() { return amenities; }
    public void setAmenities(String[] amenities) { this.amenities = amenities; }

    public String[] getTags() { return tags; }
    public void setTags(String[] tags) { this.tags = tags; }

    public Integer getPopularity() { return popularity; }
    public void setPopularity(Integer popularity) { this.popularity = popularity; }

    public String getNaverPlaceId() { return naverPlaceId; }
    public void setNaverPlaceId(String naverPlaceId) { this.naverPlaceId = naverPlaceId; }

    public String getGooglePlaceId() { return googlePlaceId; }
    public void setGooglePlaceId(String googlePlaceId) { this.googlePlaceId = googlePlaceId; }

    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }

    public String getWebsiteUrl() { return websiteUrl; }
    public void setWebsiteUrl(String websiteUrl) { this.websiteUrl = websiteUrl; }

    public String getOpeningHours() { return openingHours; }
    public void setOpeningHours(String openingHours) { this.openingHours = openingHours; }

    public String[] getTypes() { return types; }
    public void setTypes(String[] types) { this.types = types; }

    public Integer getUserRatingsTotal() { return userRatingsTotal; }
    public void setUserRatingsTotal(Integer userRatingsTotal) { this.userRatingsTotal = userRatingsTotal; }

    public Integer getPriceLevel() { return priceLevel; }
    public void setPriceLevel(Integer priceLevel) { this.priceLevel = priceLevel; }

    public String getSourceFlags() { return sourceFlags; }
    public void setSourceFlags(String sourceFlags) { this.sourceFlags = sourceFlags; }

    public LocalDate getOpenedDate() { return openedDate; }
    public void setOpenedDate(LocalDate openedDate) { this.openedDate = openedDate; }

    public ZonedDateTime getFirstSeenAt() { return firstSeenAt; }
    public void setFirstSeenAt(ZonedDateTime firstSeenAt) { this.firstSeenAt = firstSeenAt; }

    public ZonedDateTime getLastRatingCheck() { return lastRatingCheck; }
    public void setLastRatingCheck(ZonedDateTime lastRatingCheck) { this.lastRatingCheck = lastRatingCheck; }

    public Boolean getIsNewPlace() { return isNewPlace; }
    public void setIsNewPlace(Boolean isNewPlace) { this.isNewPlace = isNewPlace; }

    public Boolean getShouldRecheckRating() { return shouldRecheckRating; }
    public void setShouldRecheckRating(Boolean shouldRecheckRating) { this.shouldRecheckRating = shouldRecheckRating; }

    public String getKeywordVector() { return keywordVector; }
    public void setKeywordVector(String keywordVector) { this.keywordVector = keywordVector; }

    public ZonedDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(ZonedDateTime createdAt) { this.createdAt = createdAt; }

    public ZonedDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(ZonedDateTime updatedAt) { this.updatedAt = updatedAt; }

    public List<PlaceImage> getPlaceImages() { return placeImages; }
    public void setPlaceImages(List<PlaceImage> placeImages) { this.placeImages = placeImages; }

    // Helper methods
    public void addPlaceImage(PlaceImage placeImage) {
        placeImages.add(placeImage);
        placeImage.setPlace(this);
    }

    public void removePlaceImage(PlaceImage placeImage) {
        placeImages.remove(placeImage);
        placeImage.setPlace(null);
    }

    @Override
    public String toString() {
        return String.format("Place{id=%d, name='%s', category='%s', rating=%s}",
                id, name, category, rating);
    }

    // Builder class
    public static class Builder {
        private final Place place = new Place();

        public Builder name(String name) {
            place.setName(name);
            return this;
        }

        public Builder address(String address) {
            place.setAddress(address);
            return this;
        }

        public Builder roadAddress(String roadAddress) {
            place.setRoadAddress(roadAddress);
            return this;
        }

        public Builder latitude(BigDecimal latitude) {
            place.setLatitude(latitude);
            return this;
        }

        public Builder longitude(BigDecimal longitude) {
            place.setLongitude(longitude);
            return this;
        }

        public Builder category(String category) {
            place.setCategory(category);
            return this;
        }

        public Builder description(String description) {
            place.setDescription(description);
            return this;
        }

        public Builder imageUrl(String imageUrl) {
            place.setImageUrl(imageUrl);
            return this;
        }

        public Builder rating(BigDecimal rating) {
            place.setRating(rating);
            return this;
        }

        public Builder reviewCount(Integer reviewCount) {
            place.setReviewCount(reviewCount);
            return this;
        }

        public Builder naverPlaceId(String naverPlaceId) {
            place.setNaverPlaceId(naverPlaceId);
            return this;
        }

        public Builder googlePlaceId(String googlePlaceId) {
            place.setGooglePlaceId(googlePlaceId);
            return this;
        }

        public Builder phone(String phone) {
            place.setPhone(phone);
            return this;
        }

        public Builder websiteUrl(String websiteUrl) {
            place.setWebsiteUrl(websiteUrl);
            return this;
        }

        public Builder types(String[] types) {
            place.setTypes(types);
            return this;
        }

        public Builder userRatingsTotal(Integer userRatingsTotal) {
            place.setUserRatingsTotal(userRatingsTotal);
            return this;
        }

        public Builder sourceFlags(String sourceFlags) {
            place.setSourceFlags(sourceFlags);
            return this;
        }

        public Place build() {
            if (place.getFirstSeenAt() == null) {
                place.setFirstSeenAt(ZonedDateTime.now());
            }
            return place;
        }
    }
}
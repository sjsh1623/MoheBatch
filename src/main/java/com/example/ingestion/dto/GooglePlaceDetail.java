package com.example.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Google Place API response
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GooglePlaceDetail {

    @JsonProperty("place_id")
    private String placeId;

    @JsonProperty("name")
    private String name;

    @JsonProperty("formatted_address")
    private String formattedAddress;

    @JsonProperty("formatted_phone_number")
    private String formattedPhoneNumber;

    @JsonProperty("website")
    private String website;

    @JsonProperty("rating")
    private Double rating;

    @JsonProperty("user_ratings_total")
    private Integer userRatingsTotal;

    @JsonProperty("price_level")
    private Integer priceLevel;

    @JsonProperty("types")
    private List<String> types;

    @JsonProperty("opening_hours")
    private OpeningHours openingHours;

    @JsonProperty("reviews")
    private List<Review> reviews;

    // Default constructor
    public GooglePlaceDetail() {}

    // Getters and Setters
    public String getPlaceId() { return placeId; }
    public void setPlaceId(String placeId) { this.placeId = placeId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getFormattedAddress() { return formattedAddress; }
    public void setFormattedAddress(String formattedAddress) { this.formattedAddress = formattedAddress; }

    public String getFormattedPhoneNumber() { return formattedPhoneNumber; }
    public void setFormattedPhoneNumber(String formattedPhoneNumber) { this.formattedPhoneNumber = formattedPhoneNumber; }

    public String getWebsite() { return website; }
    public void setWebsite(String website) { this.website = website; }

    public Double getRating() { return rating; }
    public void setRating(Double rating) { this.rating = rating; }

    public Integer getUserRatingsTotal() { return userRatingsTotal; }
    public void setUserRatingsTotal(Integer userRatingsTotal) { this.userRatingsTotal = userRatingsTotal; }

    public Integer getPriceLevel() { return priceLevel; }
    public void setPriceLevel(Integer priceLevel) { this.priceLevel = priceLevel; }

    public List<String> getTypes() { return types; }
    public void setTypes(List<String> types) { this.types = types; }

    public OpeningHours getOpeningHours() { return openingHours; }
    public void setOpeningHours(OpeningHours openingHours) { this.openingHours = openingHours; }

    public List<Review> getReviews() { return reviews; }
    public void setReviews(List<Review> reviews) { this.reviews = reviews; }

    // Nested classes
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OpeningHours {
        @JsonProperty("open_now")
        private Boolean openNow;

        @JsonProperty("weekday_text")
        private List<String> weekdayText;

        public Boolean getOpenNow() { return openNow; }
        public void setOpenNow(Boolean openNow) { this.openNow = openNow; }

        public List<String> getWeekdayText() { return weekdayText; }
        public void setWeekdayText(List<String> weekdayText) { this.weekdayText = weekdayText; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Review {
        @JsonProperty("author_name")
        private String authorName;

        @JsonProperty("rating")
        private Integer rating;

        @JsonProperty("text")
        private String text;

        @JsonProperty("time")
        private Long time;

        public String getAuthorName() { return authorName; }
        public void setAuthorName(String authorName) { this.authorName = authorName; }

        public Integer getRating() { return rating; }
        public void setRating(Integer rating) { this.rating = rating; }

        public String getText() { return text; }
        public void setText(String text) { this.text = text; }

        public Long getTime() { return time; }
        public void setTime(Long time) { this.time = time; }
    }

    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    // Helper methods for compatibility
    public double getLatitude() { return 0.0; } // Will be set by Google API geometry
    public double getLongitude() { return 0.0; } // Will be set by Google API geometry
    public String getAddress() { return formattedAddress; }
    public String getPhoneNumber() { return formattedPhoneNumber; }
    public List<String> getPhotoReferences() { return List.of(); } // Will be implemented if needed
    public String getOpeningHoursText() {
        if (openingHours != null && openingHours.getWeekdayText() != null) {
            return String.join("; ", openingHours.getWeekdayText());
        }
        return "";
    }

    @Override
    public String toString() {
        return String.format("GooglePlaceDetail{placeId='%s', name='%s', rating=%s}",
                           placeId, name, rating);
    }

    // Builder class
    public static class Builder {
        private String placeId;
        private String name;
        private String formattedAddress;
        private String formattedPhoneNumber;
        private String website;
        private Double rating;
        private Integer userRatingsTotal;
        private Integer priceLevel;
        private List<String> types;
        private OpeningHours openingHours;
        private List<Review> reviews;

        public Builder placeId(String placeId) {
            this.placeId = placeId;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder address(String address) {
            this.formattedAddress = address;
            return this;
        }

        public Builder latitude(double latitude) {
            // For compatibility, but not stored in this DTO
            return this;
        }

        public Builder longitude(double longitude) {
            // For compatibility, but not stored in this DTO
            return this;
        }

        public Builder rating(double rating) {
            this.rating = rating;
            return this;
        }

        public Builder userRatingsTotal(int userRatingsTotal) {
            this.userRatingsTotal = userRatingsTotal;
            return this;
        }

        public Builder priceLevel(int priceLevel) {
            this.priceLevel = priceLevel;
            return this;
        }

        public Builder types(List<String> types) {
            this.types = types;
            return this;
        }

        public Builder phoneNumber(String phoneNumber) {
            this.formattedPhoneNumber = phoneNumber;
            return this;
        }

        public Builder website(String website) {
            this.website = website;
            return this;
        }

        public Builder photoReferences(List<String> photoReferences) {
            // For compatibility, but not stored in this DTO
            return this;
        }

        public Builder openingHours(String openingHours) {
            // For compatibility, but not stored in this DTO
            return this;
        }

        public Builder reviews(List<String> reviews) {
            // For compatibility, but not stored in this DTO
            return this;
        }

        public GooglePlaceDetail build() {
            GooglePlaceDetail detail = new GooglePlaceDetail();
            detail.setPlaceId(this.placeId);
            detail.setName(this.name);
            detail.setFormattedAddress(this.formattedAddress);
            detail.setFormattedPhoneNumber(this.formattedPhoneNumber);
            detail.setWebsite(this.website);
            detail.setRating(this.rating);
            detail.setUserRatingsTotal(this.userRatingsTotal);
            detail.setPriceLevel(this.priceLevel);
            detail.setTypes(this.types);
            detail.setOpeningHours(this.openingHours);
            detail.setReviews(this.reviews);
            return detail;
        }
    }
}
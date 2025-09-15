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

    @Override
    public String toString() {
        return String.format("GooglePlaceDetail{placeId='%s', name='%s', rating=%s}",
                           placeId, name, rating);
    }
}
package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

public class RecentPlaceData {
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("title")
    private String title;
    
    @JsonProperty("location")
    private String location;
    
    @JsonProperty("image")
    private String image;
    
    @JsonProperty("rating")
    private Double rating;
    
    @JsonProperty("viewed_at")
    private LocalDateTime viewedAt;
    
    @JsonProperty("place")
    private PlaceDto place;
    
    @JsonProperty("last_viewed_at")
    private LocalDateTime lastViewedAt;
    
    @JsonProperty("view_count")
    private int viewCount;
    
    // Default constructor
    public RecentPlaceData() {}
    
    // Constructor with fields
    public RecentPlaceData(PlaceDto place, LocalDateTime lastViewedAt, int viewCount) {
        this.place = place;
        this.lastViewedAt = lastViewedAt;
        this.viewCount = viewCount;
    }
    
    // Static factory method
    public static RecentPlaceData of(PlaceDto place, LocalDateTime lastViewedAt, int viewCount) {
        return new RecentPlaceData(place, lastViewedAt, viewCount);
    }
    
    // Getters and setters for new fields
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getTitle() {
        return title;
    }
    
    public void setTitle(String title) {
        this.title = title;
    }
    
    public String getLocation() {
        return location;
    }
    
    public void setLocation(String location) {
        this.location = location;
    }
    
    public String getImage() {
        return image;
    }
    
    public void setImage(String image) {
        this.image = image;
    }
    
    public Double getRating() {
        return rating;
    }
    
    public void setRating(BigDecimal rating) {
        this.rating = rating != null ? rating.doubleValue() : null;
    }
    
    public void setRating(Double rating) {
        this.rating = rating;
    }
    
    public LocalDateTime getViewedAt() {
        return viewedAt;
    }
    
    public void setViewedAt(OffsetDateTime viewedAt) {
        this.viewedAt = viewedAt != null ? viewedAt.toLocalDateTime() : null;
    }
    
    public void setViewedAt(LocalDateTime viewedAt) {
        this.viewedAt = viewedAt;
    }
    
    // Existing getters and setters
    public PlaceDto getPlace() {
        return place;
    }
    
    public void setPlace(PlaceDto place) {
        this.place = place;
    }
    
    public LocalDateTime getLastViewedAt() {
        return lastViewedAt;
    }
    
    public void setLastViewedAt(LocalDateTime lastViewedAt) {
        this.lastViewedAt = lastViewedAt;
    }
    
    public int getViewCount() {
        return viewCount;
    }
    
    public void setViewCount(int viewCount) {
        this.viewCount = viewCount;
    }
}
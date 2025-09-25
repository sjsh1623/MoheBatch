package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class RecentPlacesResponse {
    
    @JsonProperty("recent_places")
    private List<RecentPlaceData> recentPlaces;
    
    @JsonProperty("total_count")
    private int totalCount;
    
    // Default constructor
    public RecentPlacesResponse() {}
    
    // Constructor with fields
    public RecentPlacesResponse(List<RecentPlaceData> recentPlaces, int totalCount) {
        this.recentPlaces = recentPlaces;
        this.totalCount = totalCount;
    }
    
    // Static factory method
    public static RecentPlacesResponse of(List<RecentPlaceData> recentPlaces) {
        return new RecentPlacesResponse(recentPlaces, recentPlaces != null ? recentPlaces.size() : 0);
    }
    
    // Getters and setters
    public List<RecentPlaceData> getRecentPlaces() {
        return recentPlaces;
    }
    
    public void setRecentPlaces(List<RecentPlaceData> recentPlaces) {
        this.recentPlaces = recentPlaces;
    }
    
    public int getTotalCount() {
        return totalCount;
    }
    
    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }
}
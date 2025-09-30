package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class MyPlacesResponse {
    
    @JsonProperty("places")
    private List<SimplePlaceDto> places;
    
    @JsonProperty("total_count")
    private int totalCount;
    
    @JsonProperty("page")
    private int page;
    
    @JsonProperty("size")
    private int size;
    
    @JsonProperty("has_next")
    private boolean hasNext;
    
    // Default constructor
    public MyPlacesResponse() {}
    
    // Constructor with fields
    public MyPlacesResponse(List<SimplePlaceDto> places, int totalCount, int page, int size, boolean hasNext) {
        this.places = places;
        this.totalCount = totalCount;
        this.page = page;
        this.size = size;
        this.hasNext = hasNext;
    }
    
    // Static factory method
    public static MyPlacesResponse of(List<SimplePlaceDto> places, int totalCount, int page, int size, boolean hasNext) {
        return new MyPlacesResponse(places, totalCount, page, size, hasNext);
    }
    
    // Getters and setters
    public List<SimplePlaceDto> getPlaces() {
        return places;
    }
    
    public void setPlaces(List<SimplePlaceDto> places) {
        this.places = places;
    }
    
    // Alias for backward compatibility
    public void setMyPlaces(List<SimplePlaceDto> places) {
        this.places = places;
    }
    
    public int getTotalCount() {
        return totalCount;
    }
    
    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }
    
    public int getPage() {
        return page;
    }
    
    public void setPage(int page) {
        this.page = page;
    }
    
    public int getSize() {
        return size;
    }
    
    public void setSize(int size) {
        this.size = size;
    }
    
    public boolean isHasNext() {
        return hasNext;
    }
    
    public void setHasNext(boolean hasNext) {
        this.hasNext = hasNext;
    }
}

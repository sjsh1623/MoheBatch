package com.mohe.spring.dto;

import java.util.List;
import java.util.Map;

public class EnhancedRecommendationsResponse {
    private List<PlaceDto> places;
    private Map<String, Object> metadata;
    private String algorithm;
    private int totalCount;

    public EnhancedRecommendationsResponse() {}

    public EnhancedRecommendationsResponse(List<PlaceDto> places, Map<String, Object> metadata, String algorithm, int totalCount) {
        this.places = places;
        this.metadata = metadata;
        this.algorithm = algorithm;
        this.totalCount = totalCount;
    }

    public List<PlaceDto> getPlaces() {
        return places;
    }

    public void setPlaces(List<PlaceDto> places) {
        this.places = places;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }
}
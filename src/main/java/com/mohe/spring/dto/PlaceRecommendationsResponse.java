package com.mohe.spring.dto;

import java.util.List;

public class PlaceRecommendationsResponse {
    private List<PlaceDto> recommendations;
    private int totalCount;
    private String algorithm;

    public PlaceRecommendationsResponse() {}

    public PlaceRecommendationsResponse(List<PlaceDto> recommendations, int totalCount, String algorithm) {
        this.recommendations = recommendations;
        this.totalCount = totalCount;
        this.algorithm = algorithm;
    }

    public List<PlaceDto> getRecommendations() {
        return recommendations;
    }

    public void setRecommendations(List<PlaceDto> recommendations) {
        this.recommendations = recommendations;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }
}
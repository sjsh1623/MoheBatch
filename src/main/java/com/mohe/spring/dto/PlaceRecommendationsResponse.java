package com.mohe.spring.dto;

import java.util.List;

public class PlaceRecommendationsResponse {
    private List<SimplePlaceDto> recommendations;
    private int totalCount;
    private String algorithm;

    public PlaceRecommendationsResponse() {}

    public PlaceRecommendationsResponse(List<SimplePlaceDto> recommendations, int totalCount, String algorithm) {
        this.recommendations = recommendations;
        this.totalCount = totalCount;
        this.algorithm = algorithm;
    }

    public List<SimplePlaceDto> getRecommendations() {
        return recommendations;
    }

    public void setRecommendations(List<SimplePlaceDto> recommendations) {
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
package com.mohe.spring.dto;

import java.util.List;
import java.util.Map;

public class ContextualRecommendationResponse {
    private List<PlaceDto> places;
    private Map<String, Object> context;
    private String weatherCondition;
    private String timeContext;
    private String recommendation;

    public ContextualRecommendationResponse() {}

    public ContextualRecommendationResponse(List<PlaceDto> places, Map<String, Object> context, 
                                          String weatherCondition, String timeContext, String recommendation) {
        this.places = places;
        this.context = context;
        this.weatherCondition = weatherCondition;
        this.timeContext = timeContext;
        this.recommendation = recommendation;
    }

    public List<PlaceDto> getPlaces() {
        return places;
    }

    public void setPlaces(List<PlaceDto> places) {
        this.places = places;
    }

    public Map<String, Object> getContext() {
        return context;
    }

    public void setContext(Map<String, Object> context) {
        this.context = context;
    }

    public String getWeatherCondition() {
        return weatherCondition;
    }

    public void setWeatherCondition(String weatherCondition) {
        this.weatherCondition = weatherCondition;
    }

    public String getTimeContext() {
        return timeContext;
    }

    public void setTimeContext(String timeContext) {
        this.timeContext = timeContext;
    }

    public String getRecommendation() {
        return recommendation;
    }

    public void setRecommendation(String recommendation) {
        this.recommendation = recommendation;
    }
}
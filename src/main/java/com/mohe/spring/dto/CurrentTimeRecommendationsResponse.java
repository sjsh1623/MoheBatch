package com.mohe.spring.dto;

import java.util.List;
import java.util.Map;

public class CurrentTimeRecommendationsResponse {
    private List<PlaceDto> recommendations;
    private Map<String, Object> timeContext;
    private String weatherCondition;
    private String timeOfDay;

    public CurrentTimeRecommendationsResponse() {}

    public CurrentTimeRecommendationsResponse(List<PlaceDto> recommendations, Map<String, Object> timeContext, 
                                            String weatherCondition, String timeOfDay) {
        this.recommendations = recommendations;
        this.timeContext = timeContext;
        this.weatherCondition = weatherCondition;
        this.timeOfDay = timeOfDay;
    }

    public List<PlaceDto> getRecommendations() {
        return recommendations;
    }

    public void setRecommendations(List<PlaceDto> recommendations) {
        this.recommendations = recommendations;
    }

    public Map<String, Object> getTimeContext() {
        return timeContext;
    }

    public void setTimeContext(Map<String, Object> timeContext) {
        this.timeContext = timeContext;
    }

    public String getWeatherCondition() {
        return weatherCondition;
    }

    public void setWeatherCondition(String weatherCondition) {
        this.weatherCondition = weatherCondition;
    }

    public String getTimeOfDay() {
        return timeOfDay;
    }

    public void setTimeOfDay(String timeOfDay) {
        this.timeOfDay = timeOfDay;
    }
}
package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class WeatherBasedRecommendationResponse {
    
    @JsonProperty("places")
    private List<PlaceDto> places;
    
    @JsonProperty("weather_condition")
    private String weatherCondition;
    
    @JsonProperty("temperature")
    private Integer temperature;
    
    @JsonProperty("indoor_ratio")
    private Double indoorRatio;
    
    @JsonProperty("recommendation_reason")
    private String recommendationReason;
    
    @JsonProperty("total_count")
    private int totalCount;
    
    // Default constructor
    public WeatherBasedRecommendationResponse() {}
    
    // Constructor with fields
    public WeatherBasedRecommendationResponse(List<PlaceDto> places, String weatherCondition,
                                            Integer temperature, Double indoorRatio,
                                            String recommendationReason, int totalCount) {
        this.places = places;
        this.weatherCondition = weatherCondition;
        this.temperature = temperature;
        this.indoorRatio = indoorRatio;
        this.recommendationReason = recommendationReason;
        this.totalCount = totalCount;
    }
    
    // Static factory method
    public static WeatherBasedRecommendationResponse of(List<PlaceDto> places, String weatherCondition,
                                                      Integer temperature, Double indoorRatio,
                                                      String recommendationReason) {
        return new WeatherBasedRecommendationResponse(places, weatherCondition, temperature,
                                                    indoorRatio, recommendationReason,
                                                    places != null ? places.size() : 0);
    }
    
    // Getters and setters
    public List<PlaceDto> getPlaces() {
        return places;
    }
    
    public void setPlaces(List<PlaceDto> places) {
        this.places = places;
    }
    
    public String getWeatherCondition() {
        return weatherCondition;
    }
    
    public void setWeatherCondition(String weatherCondition) {
        this.weatherCondition = weatherCondition;
    }
    
    public Integer getTemperature() {
        return temperature;
    }
    
    public void setTemperature(Integer temperature) {
        this.temperature = temperature;
    }
    
    public Double getIndoorRatio() {
        return indoorRatio;
    }
    
    public void setIndoorRatio(Double indoorRatio) {
        this.indoorRatio = indoorRatio;
    }
    
    public String getRecommendationReason() {
        return recommendationReason;
    }
    
    public void setRecommendationReason(String recommendationReason) {
        this.recommendationReason = recommendationReason;
    }
    
    public int getTotalCount() {
        return totalCount;
    }
    
    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }
}
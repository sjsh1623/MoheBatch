package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public class WeatherBasedRecommendationRequest {
    
    @JsonProperty("weather_condition")
    @NotBlank(message = "날씨 상태는 필수입니다")
    private String weatherCondition;
    
    @JsonProperty("temperature")
    @Min(value = -30, message = "온도는 -30도 이상이어야 합니다")
    @Max(value = 50, message = "온도는 50도 이하여야 합니다")
    private Integer temperature;
    
    @JsonProperty("humidity")
    @Min(value = 0, message = "습도는 0% 이상이어야 합니다")
    @Max(value = 100, message = "습도는 100% 이하여야 합니다")
    private Integer humidity;
    
    @JsonProperty("indoor_preferred")
    private Boolean indoorPreferred;
    
    // Default constructor
    public WeatherBasedRecommendationRequest() {}
    
    // Constructor with fields
    public WeatherBasedRecommendationRequest(String weatherCondition, Integer temperature, 
                                           Integer humidity, Boolean indoorPreferred) {
        this.weatherCondition = weatherCondition;
        this.temperature = temperature;
        this.humidity = humidity;
        this.indoorPreferred = indoorPreferred;
    }
    
    // Getters and setters
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
    
    public Integer getHumidity() {
        return humidity;
    }
    
    public void setHumidity(Integer humidity) {
        this.humidity = humidity;
    }
    
    public Boolean getIndoorPreferred() {
        return indoorPreferred;
    }
    
    public void setIndoorPreferred(Boolean indoorPreferred) {
        this.indoorPreferred = indoorPreferred;
    }
}

package com.mohe.spring.service;

import java.util.List;

public class OllamaRecommendationResponse {
    private boolean success;
    private List<String> recommendedPlaces = new java.util.ArrayList<>();
    private String reasoning;
    private String rawResponse;

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public List<String> getRecommendedPlaces() { return recommendedPlaces; }
    public void setRecommendedPlaces(List<String> recommendedPlaces) { this.recommendedPlaces = recommendedPlaces; }
    public String getReasoning() { return reasoning; }
    public void setReasoning(String reasoning) { this.reasoning = reasoning; }
    public String getRawResponse() { return rawResponse; }
    public void setRawResponse(String rawResponse) { this.rawResponse = rawResponse; }
}

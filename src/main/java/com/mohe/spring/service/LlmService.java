
package com.mohe.spring.service;

import java.util.List;

public interface LlmService {
        OllamaRecommendationResponse generatePlaceRecommendations(String prompt, List<String> availablePlaces);

        OllamaRecommendationResponse generatePlaceRecommendations(
                String userLocation, String weatherCondition, String timeOfDay,
                String userMbti, List<String> availablePlaces);
}

package com.mohe.spring.service;

import com.mohe.spring.dto.ContextualRecommendationResponse;
import com.mohe.spring.dto.SimplePlaceDto;
import com.mohe.spring.dto.PlaceDto;
import com.mohe.spring.entity.Place;
import com.mohe.spring.entity.User;
import com.mohe.spring.repository.PlaceRepository;
import com.mohe.spring.repository.UserRepository;
import com.mohe.spring.service.OllamaRecommendationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class ContextualRecommendationService {
    
    private static final Logger logger = LoggerFactory.getLogger(ContextualRecommendationService.class);
    
    private final PlaceRepository placeRepository;
    private final UserRepository userRepository;
    private final WeatherService weatherService;
    private final LlmService llmService;
    
    @Autowired
    public ContextualRecommendationService(
            PlaceRepository placeRepository,
            UserRepository userRepository,
            WeatherService weatherService,
            LlmService llmService) {
        this.placeRepository = placeRepository;
        this.userRepository = userRepository;
        this.weatherService = weatherService;
        this.llmService = llmService;
    }
    
    /**
     * Get contextual recommendations based on user authentication status
     */
    public ContextualRecommendationResponse getContextualRecommendations(
            String query, Double lat, Double lon, int limit) {
        
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        boolean isAuthenticated = auth != null && auth.isAuthenticated() && !auth.getName().equals("anonymousUser");
        
        if (isAuthenticated) {
            return getAuthenticatedRecommendations(query, lat, lon, limit, auth.getName());
        } else {
            return getUnauthenticatedRecommendations(query, lat, lon, limit);
        }
    }
    
    /**
     * For unauthenticated users: Location + weather + time → LLM recommendations within 20km
     */
    private ContextualRecommendationResponse getUnauthenticatedRecommendations(
            String query, Double lat, Double lon, int limit) {
        
        // Get current weather and time context
        String weatherCondition = "맑음"; // Default
        String timeOfDay = getCurrentTimeOfDay();
        String userLocation = "서울"; // Default location
        
        // Get weather data if location is provided
        if (lat != null && lon != null) {
            try {
                var weatherData = weatherService.getCurrentWeather(lat, lon);
                weatherCondition = weatherData.getConditionText();
                userLocation = String.format("위도 %.4f, 경도 %.4f 지역", lat, lon);
                logger.info("Successfully retrieved weather data: {} for location: {}", weatherCondition, userLocation);
            } catch (Exception e) {
                logger.warn("Failed to retrieve weather data for lat={}, lon={}: {}", lat, lon, e.getMessage());
                weatherCondition = "날씨 정보를 가져올 수 없음";
            }
        }
        
        // Get places within 20km radius
        List<Place> nearbyPlaces;
        if (lat != null && lon != null) {
            // Use the new findPopularPlaces method with 20km radius
            nearbyPlaces = placeRepository.findPopularPlaces(lat, lon, 20.0);
        } else {
            // No location provided, get recommendable places
            nearbyPlaces = placeRepository.findRecommendablePlaces(PageRequest.of(0, limit * 3)).getContent();
        }
        
        // Filter to good places and prepare for LLM
        List<Place> candidatePlaces = nearbyPlaces.stream()
            .filter(place -> place.getRating().doubleValue() >= 3.0)
            .limit(50) // Limit candidates for LLM processing
            .collect(Collectors.toList());
        
        // Prepare place names for LLM
        List<String> placeNames = candidatePlaces.stream()
            .map(place -> place.getName() != null ? place.getName() : place.getTitle())
            .collect(Collectors.toList());
        
        // Use LLM for intelligent recommendations
        OllamaRecommendationResponse llmResponse = null;
        boolean llmSuccess = false;
        
        try {
            llmResponse = llmService.generatePlaceRecommendations(
                userLocation, weatherCondition, timeOfDay, null, placeNames);
            llmSuccess = llmResponse.isSuccess();
        } catch (Exception e) {
            logger.warn("LLM failed, using fallback: {}", e.getMessage());
        }
        
        // Process recommendations
        List<SimplePlaceDto> recommendations;
        String contextMessage;
        
        if (llmSuccess && llmResponse != null) {
            // Use LLM recommendations
            final OllamaRecommendationResponse finalLlmResponse = llmResponse;
            recommendations = candidatePlaces.stream()
                .filter(place -> {
                    String placeName = place.getName() != null ? place.getName() : place.getTitle();
                    return finalLlmResponse.getRecommendedPlaces().contains(placeName);
                })
                .limit(Math.min(limit, 15))
                .map(this::convertToSimplePlaceDto)
                .collect(Collectors.toList());
            
            contextMessage = String.format("AI가 %s, %s 시간대 상황을 고려하여 추천한 장소입니다. %s", 
                weatherCondition, timeOfDay, 
                llmResponse.getReasoning() != null ? llmResponse.getReasoning() : "");
        } else {
            // Fallback to basic recommendations
            recommendations = candidatePlaces.stream()
                .sorted((p1, p2) -> Double.compare(p2.getRating().doubleValue(), p1.getRating().doubleValue()))
                .limit(Math.min(limit, 15))
                .map(this::convertToSimplePlaceDto)
                .collect(Collectors.toList());
            
            contextMessage = String.format("%s, %s 시간대에 좋은 장소들을 추천드립니다. (기본 추천)", 
                weatherCondition, timeOfDay);
        }
        
        // Create context map
        Map<String, Object> context = Map.of(
            "weather", weatherCondition,
            "timeOfDay", timeOfDay,
            "recommendation", contextMessage,
            "llmUsed", llmSuccess,
            "location", userLocation
        );
        
        return new ContextualRecommendationResponse(
            recommendations.stream()
                .map(place -> new PlaceDto.PlaceResponse(
                    Long.parseLong(place.getId()),
                    place.getName(),
                    place.getImageUrl(),
                    place.getImages(),
                    place.getRating(),
                    place.getCategory()
                ))
                .collect(java.util.stream.Collectors.toList()),
            context,
            weatherCondition,
            timeOfDay,
            contextMessage
        );
    }
    
    /**
     * For authenticated users: User preferences + MBTI + LLM recommendations within 20km
     */
    private ContextualRecommendationResponse getAuthenticatedRecommendations(
            String query, Double lat, Double lon, int limit, String username) {
        
        try {
            // Get user preferences and MBTI
            User user = userRepository.findByEmail(username).orElse(null);
            if (user == null) {
                // Fallback to unauthenticated recommendations
                return getUnauthenticatedRecommendations(query, lat, lon, limit);
            }
            
            String mbtiType = user.getMbti();
            String weatherCondition = "맑음";
            String timeOfDay = getCurrentTimeOfDay();
            String userLocation = "서울"; // Default location
            
            // Get weather data if location is provided
            if (lat != null && lon != null) {
                try {
                    var weatherData = weatherService.getCurrentWeather(lat, lon);
                    weatherCondition = weatherData.getConditionText();
                    userLocation = String.format("위도 %.4f, 경도 %.4f 지역", lat, lon);
                    logger.info("Successfully retrieved weather data: {} for authenticated user at location: {}", weatherCondition, userLocation);
                } catch (Exception e) {
                    logger.warn("Failed to retrieve weather data for authenticated user lat={}, lon={}: {}", lat, lon, e.getMessage());
                    weatherCondition = "날씨 정보를 가져올 수 없음";
                }
            }
            
            // Get places within 20km radius
            List<Place> nearbyPlaces;
            if (lat != null && lon != null) {
                // Use the new findPopularPlaces method with 20km radius
                nearbyPlaces = placeRepository.findPopularPlaces(lat, lon, 20.0);
            } else {
                // No location provided, get recommendable places
                nearbyPlaces = placeRepository.findRecommendablePlaces(PageRequest.of(0, limit * 3)).getContent();
            }
            
            // Filter to good places and prepare for LLM
            List<Place> candidatePlaces = nearbyPlaces.stream()
                .filter(place -> place.getRating().doubleValue() >= 3.0)
                .limit(50) // Limit candidates for LLM processing
                .collect(Collectors.toList());
            
            // Prepare place names for LLM
            List<String> placeNames = candidatePlaces.stream()
                .map(place -> place.getName() != null ? place.getName() : place.getTitle())
                .collect(Collectors.toList());
            
            // Use LLM for intelligent recommendations with MBTI
            OllamaRecommendationResponse llmResponse = null;
            boolean llmSuccess = false;
            
            try {
                llmResponse = llmService.generatePlaceRecommendations(
                    userLocation, weatherCondition, timeOfDay, mbtiType, placeNames);
                llmSuccess = llmResponse.isSuccess();
            } catch (Exception e) {
                logger.warn("LLM failed for authenticated user, using fallback: {}", e.getMessage());
            }
            
            // Process recommendations
            List<SimplePlaceDto> recommendations;
            String contextMessage;
            
            if (llmSuccess && llmResponse != null) {
                // Use LLM recommendations
                final OllamaRecommendationResponse finalLlmResponse = llmResponse;
                recommendations = candidatePlaces.stream()
                    .filter(place -> {
                        String placeName = place.getName() != null ? place.getName() : place.getTitle();
                        return finalLlmResponse.getRecommendedPlaces().contains(placeName);
                    })
                    .limit(Math.min(limit, 15))
                    .map(this::convertToSimplePlaceDto)
                    .collect(Collectors.toList());
                
                contextMessage = String.format("AI가 MBTI %s 성향과 %s, %s 시간대를 고려하여 추천한 장소입니다. %s", 
                    mbtiType != null ? mbtiType : "Unknown", weatherCondition, timeOfDay,
                    llmResponse.getReasoning() != null ? llmResponse.getReasoning() : "");
            } else {
                // Fallback to MBTI-based recommendations
                recommendations = candidatePlaces.stream()
                    .sorted((p1, p2) -> {
                        // Prefer places that might match MBTI preferences
                        double score1 = calculateMbtiScore(p1, mbtiType);
                        double score2 = calculateMbtiScore(p2, mbtiType);
                        if (score1 != score2) return Double.compare(score2, score1);
                        return Double.compare(p2.getRating().doubleValue(), p1.getRating().doubleValue());
                    })
                    .limit(Math.min(limit, 15))
                    .map(this::convertToSimplePlaceDto)
                    .collect(Collectors.toList());
                
                contextMessage = String.format("MBTI %s 성향에 맞는 %s, %s 시간대 추천 장소입니다. (기본 추천)", 
                    mbtiType != null ? mbtiType : "Unknown", weatherCondition, timeOfDay);
            }
            
            // Create context map
            Map<String, Object> context = Map.of(
                "weather", weatherCondition,
                "timeOfDay", timeOfDay,
                "recommendation", contextMessage,
                "mbtiType", mbtiType != null ? mbtiType : "Unknown",
                "llmUsed", llmSuccess,
                "location", userLocation
            );
            
            return new ContextualRecommendationResponse(
                recommendations.stream()
                    .map(place -> new PlaceDto.PlaceResponse(
                        Long.parseLong(place.getId()),
                        place.getName(),
                        place.getImageUrl(),
                        place.getImages(),
                        place.getRating(),
                        place.getCategory()
                    ))
                    .collect(java.util.stream.Collectors.toList()),
                context,
                weatherCondition,
                timeOfDay,
                contextMessage
            );
            
        } catch (Exception e) {
            // Fallback to unauthenticated recommendations on error
            return getUnauthenticatedRecommendations(query, lat, lon, limit);
        }
    }
    
    private String buildRecommendationPrompt(String userLocation, String weatherCondition, 
            String timeOfDay, String userMbti, List<String> availablePlaces) {
        
        StringBuilder prompt = new StringBuilder();
        prompt.append("당신은 한국의 장소 추천 전문가입니다. 다음 정보를 바탕으로 사용자에게 최적의 장소를 추천해주세요.\n\n");
        
        prompt.append("📍 위치 정보: ").append(userLocation).append("\n");
        prompt.append("🌤️ 날씨: ").append(weatherCondition).append("\n");
        prompt.append("⏰ 시간대: ").append(timeOfDay).append("\n");
        
        if (userMbti != null && !userMbti.isEmpty()) {
            prompt.append("👤 MBTI 성향: ").append(userMbti).append("\n");
        }
        
        prompt.append("\n가능한 장소 목록:\n");
        for (int i = 0; i < availablePlaces.size(); i++) {
            prompt.append((i + 1)).append(". ").append(availablePlaces.get(i)).append("\n");
        }
        
        prompt.append("\n요청사항:\n");
        prompt.append("1. 위의 조건들을 종합적으로 고려하여 가장 적합한 장소들을 선별해주세요\n");
        prompt.append("2. 최대 15개의 장소를 추천해주세요\n");
        prompt.append("3. 추천 이유를 간단히 설명해주세요\n");
        prompt.append("4. 응답 형식: '추천장소: [장소명1], [장소명2], [장소명3]... 이유: [추천이유]'\n");
        prompt.append("5. 장소명은 위 목록에 있는 정확한 이름을 사용해주세요\n\n");
        
        return prompt.toString();
    }

    /**
     * Simple MBTI-based scoring (can be enhanced with ML/vector similarity)
     */
    private double calculateMbtiScore(Place place, String mbtiType) {
        if (mbtiType == null || place.getCategory() == null) return 0.0;
        
        // Simple heuristic based on place category and MBTI preferences
        String category = place.getCategory().toLowerCase();
        
        // Extrovert (E) vs Introvert (I) preferences
        if (mbtiType.startsWith("E")) {
            if (category.contains("카페") || category.contains("바") || category.contains("클럽")) return 1.0;
        } else if (mbtiType.startsWith("I")) {
            if (category.contains("도서관") || category.contains("박물관") || category.contains("공원")) return 1.0;
        }
        
        // Sensing (S) vs Intuition (N) preferences
        if (mbtiType.charAt(1) == 'S') {
            if (category.contains("맛집") || category.contains("레스토랑")) return 0.8;
        } else if (mbtiType.charAt(1) == 'N') {
            if (category.contains("갤러리") || category.contains("전시")) return 0.8;
        }
        
        return 0.5; // Default score
    }
    
    private SimplePlaceDto convertToSimplePlaceDto(Place place) {
        SimplePlaceDto dto = new SimplePlaceDto(
            place.getId().toString(),
            place.getName() != null ? place.getName() : place.getTitle(),
            place.getCategory() != null ? place.getCategory() : "기타",
            place.getRating() != null ? place.getRating().doubleValue() : 4.0,
            place.getAddress(),
            getPlaceImageUrl(place)
        );
        
        // Set additional fields
        dto.setReviewCount(place.getReviewCount() != null ? place.getReviewCount() : 0);
        dto.setAddress(place.getAddress());
        dto.setImages(place.getGallery());
        dto.setDescription(place.getDescription());
        dto.setTags(place.getTags());
        dto.setPhone(place.getPhone());
        dto.setWebsiteUrl(place.getWebsiteUrl());
        dto.setAmenities(place.getAmenities());
        dto.setDistance(0.0); // Distance disabled as per requirements
        dto.setIsBookmarked(false); // TODO: Check if bookmarked by current user
        dto.setIsDemo(false);
        
        return dto;
    }
    
    private String getPlaceImageUrl(Place place) {
        if (place.getGallery() != null && !place.getGallery().isEmpty()) {
            return place.getGallery().get(0);
        }
        return null;
    }
    
    private String getCurrentTimeOfDay() {
        LocalTime now = LocalTime.now();
        int hour = now.getHour();
        if (hour >= 6 && hour < 12) return "아침";
        if (hour >= 12 && hour < 18) return "오후";
        if (hour >= 18 && hour < 22) return "저녁";
        return "밤";
    }
    
    /**
     * Get weather-based recommendations
     */
    public ContextualRecommendationResponse getWeatherBasedRecommendations(double latitude, double longitude) {
        return getContextualRecommendations(null, latitude, longitude, 20);
    }
}

package com.mohe.spring.service;

import com.mohe.spring.dto.*;
import com.mohe.spring.entity.Place;
import com.mohe.spring.repository.PlaceRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Page;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class PlaceService {
    
    private final PlaceRepository placeRepository;
    private final VectorSearchService vectorSearchService;
    
    public PlaceService(PlaceRepository placeRepository, VectorSearchService vectorSearchService) {
        this.placeRepository = placeRepository;
        this.vectorSearchService = vectorSearchService;
    }
    
    public PlaceRecommendationsResponse getRecommendations() {
        try {
            Authentication auth = SecurityContextHolder.getContext().getAuthentication();
            boolean isAuthenticated = auth != null && auth.isAuthenticated() && !auth.getName().equals("anonymousUser");
            
            List<Place> places;
            String recommendationType;
            
            if (isAuthenticated) {
                // Use vector-based personalized recommendations for authenticated users
                places = vectorSearchService.getPersonalizedRecommendations(auth.getName(), 20);
                recommendationType = "vector-personalized";
            } else {
                // Fall back to rating-based recommendations for unauthenticated users
                PageRequest pageRequest = PageRequest.of(0, 20);
                places = placeRepository.findRecommendablePlaces(pageRequest).getContent();
                recommendationType = "rating-based";
            }
            
            List<SimplePlaceDto> placeDtos = places.stream()
                .map(this::convertToSimplePlaceDto)
                .collect(Collectors.toList());
            
            return new PlaceRecommendationsResponse(placeDtos, placeDtos.size(), recommendationType);
        } catch (Exception e) {
            // Fallback to basic recommendations on error
            PageRequest pageRequest = PageRequest.of(0, 20);
            List<Place> places = placeRepository.findRecommendablePlaces(pageRequest).getContent();
            
            List<SimplePlaceDto> placeDtos = places.stream()
                .map(this::convertToSimplePlaceDto)
                .collect(Collectors.toList());
            
            return new PlaceRecommendationsResponse(placeDtos, placeDtos.size(), "rating-based-fallback");
        }
    }
    
    public PlaceListResponse getPlaces(int page, int limit, String category, String sort) {
        PageRequest pageRequest = PageRequest.of(page - 1, limit); // Convert to 0-based indexing
        
        Page<Place> placePage;
        if (category != null && !category.trim().isEmpty()) {
            placePage = placeRepository.findByCategory(category, pageRequest);
        } else {
            // Default to recommendable places sorted by rating
            placePage = placeRepository.findRecommendablePlaces(pageRequest);
        }
        
        List<SimplePlaceDto> placeDtos = placePage.getContent().stream()
            .map(this::convertToSimplePlaceDto)
            .collect(Collectors.toList());
        
        return new PlaceListResponse(placeDtos, (int) placePage.getTotalElements(), 
                                   placePage.getTotalPages(), page, limit);
    }
    
    public PlaceDetailResponse getPlaceDetail(String id) {
        Optional<Place> placeOpt = placeRepository.findById(Long.parseLong(id));
        if (placeOpt.isEmpty()) {
            throw new RuntimeException("장소를 찾을 수 없습니다: " + id);
        }
        
        Place place = placeOpt.get();
        SimplePlaceDto placeDto = convertToSimplePlaceDto(place);
        
        // Get images from gallery
        List<String> images = place.getGallery() != null && !place.getGallery().isEmpty() ? 
            place.getGallery() : 
            List.of();
        
        // Get similar places (simple implementation - same category)
        List<SimplePlaceDto> similarPlaces = List.of(); // TODO: Implement similarity logic
        
        return new PlaceDetailResponse(placeDto, images, false, similarPlaces);
    }
    
    public PlaceSearchResponse searchPlaces(String q, String location, String weather, String time) {
        try {
            Authentication auth = SecurityContextHolder.getContext().getAuthentication();
            boolean isAuthenticated = auth != null && auth.isAuthenticated() && !auth.getName().equals("anonymousUser");
            
            List<Place> searchResults;
            String searchType;
            int totalResults;
            
            if (isAuthenticated) {
                // Use vector-based personalized search for authenticated users
                VectorSimilarityResponse vectorResponse = vectorSearchService.searchWithVectorSimilarity(
                    q, auth.getName(), 0.2, 20);
                
                // Extract places from vector similarity response  
                searchResults = vectorResponse.getSimilarPlaces().stream()
                    .map(similarPlace -> {
                        // Convert PlaceResponse back to Place for consistency
                        try {
                            PlaceDto.PlaceResponse placeDto = similarPlace.getPlace();
                            Long placeId = placeDto.getId();
                            return placeRepository.findById(placeId).orElse(null);
                        } catch (Exception e) {
                            return null;
                        }
                    })
                    .filter(place -> place != null)
                    .collect(Collectors.toList());
                
                searchType = "vector-similarity";
                totalResults = vectorResponse.getTotalMatches() != null ? vectorResponse.getTotalMatches() : searchResults.size();
                
                // Fallback to database search if no vector results
                if (searchResults.isEmpty()) {
                    PageRequest pageRequest = PageRequest.of(0, 20);
                    Page<Place> searchPage = placeRepository.searchPlaces(q, pageRequest);
                    searchResults = searchPage.getContent();
                    searchType = "database-fallback";
                    totalResults = (int) searchPage.getTotalElements();
                }
                
            } else {
                // Use simple database search for unauthenticated users
                PageRequest pageRequest = PageRequest.of(0, 20);
                Page<Place> searchPage = placeRepository.searchPlaces(q, pageRequest);
                searchResults = searchPage.getContent();
                searchType = "database-search";
                totalResults = (int) searchPage.getTotalElements();
            }
            
            List<SimplePlaceDto> placeDtos = searchResults.stream()
                .map(this::convertToSimplePlaceDto)
                .collect(Collectors.toList());
            
            // Create enhanced search context
            String contextRecommendation = isAuthenticated ? 
                "개인 취향을 반영한 검색 결과입니다." : 
                "키워드 기반 검색 결과입니다.";
                
            if (weather != null && weather.equals("hot")) {
                contextRecommendation += " 더운 날씨에 적합한 실내 장소를 우선 추천합니다.";
            } else if (weather != null && weather.equals("cold")) {
                contextRecommendation += " 추운 날씨에 따뜻한 실내 장소를 우선 추천합니다.";
            }
            
            Map<String, Object> searchContext = Map.of(
                "weather", weather != null ? weather : "알 수 없음",
                "time", time != null ? time : "알 수 없음",
                "searchType", searchType,
                "recommendation", contextRecommendation
            );
            
            return new PlaceSearchResponse(placeDtos, searchContext, totalResults, q);
            
        } catch (Exception e) {
            // Fallback to basic keyword search on error
            PageRequest pageRequest = PageRequest.of(0, 20);
            Page<Place> searchResults = placeRepository.searchPlaces(q, pageRequest);
            
            List<SimplePlaceDto> placeDtos = searchResults.getContent().stream()
                .map(this::convertToSimplePlaceDto)
                .collect(Collectors.toList());
            
            Map<String, Object> searchContext = Map.of(
                "weather", weather != null ? weather : "알 수 없음",
                "time", time != null ? time : "알 수 없음",
                "searchType", "keyword-fallback",
                "recommendation", "기본 검색 결과입니다."
            );
            
            return new PlaceSearchResponse(placeDtos, searchContext, (int) searchResults.getTotalElements(), q);
        }
    }
    
    public Map<String, Object> getDebugInfo() {
        try {
            long totalPlaces = placeRepository.count();
            long recommendablePlaces = placeRepository.countRecommendablePlaces();
            List<String> categories = placeRepository.findAllCategories();
            
            return Map.of(
                "totalPlaces", totalPlaces,
                "recommendablePlaces", recommendablePlaces,
                "categories", categories,
                "timestamp", java.time.OffsetDateTime.now().toString()
            );
        } catch (Exception e) {
            // If database error, return error info
            return Map.of(
                "error", "Database error: " + e.getMessage(),
                "totalPlaces", 0,
                "recommendablePlaces", 0,
                "categories", List.of(),
                "timestamp", java.time.OffsetDateTime.now().toString()
            );
        }
    }
    
    public PlaceListResponse getPopularPlaces(double latitude, double longitude) {
        try {
            // Find popular places within 20km radius
            List<Place> popularPlaces = placeRepository.findPopularPlaces(latitude, longitude, 20.0);
            
            List<SimplePlaceDto> placeDtos = popularPlaces.stream()
                .map(this::convertToSimplePlaceDto)
                .collect(Collectors.toList());
            
            return new PlaceListResponse(placeDtos, popularPlaces.size(), 1, 1, 20);
        } catch (Exception e) {
            // Return empty result if database error
            return new PlaceListResponse(List.of(), 0, 0, 1, 20);
        }
    }
    
    public CurrentTimeRecommendationsResponse getCurrentTimePlaces(Double latitude, Double longitude, int limit) {
        try {
            List<Place> places;
            
            // If location is provided, find nearby places within 20km
            if (latitude != null && longitude != null) {
                places = placeRepository.findPopularPlaces(latitude, longitude, 20.0);
                if (places.size() > limit) {
                    places = places.subList(0, limit);
                }
            } else {
                // Fallback to general recommendations
                PageRequest pageRequest = PageRequest.of(0, limit);
                places = placeRepository.findRecommendablePlaces(pageRequest).getContent();
            }
            
            List<SimplePlaceDto> placeDtos = places.stream()
                .map(this::convertToSimplePlaceDto)
                .collect(Collectors.toList());
            
            // Create time context with better logic
            java.time.LocalTime now = java.time.LocalTime.now();
            String timeOfDay = getTimeOfDay(now);
            String weatherCondition = "맑음"; // TODO: integrate actual weather API
            String locationText = (latitude != null && longitude != null) ? "주변 " : "";
            
            Map<String, Object> timeContext = Map.of(
                "timeOfDay", timeOfDay,
                "weatherCondition", weatherCondition,
                "recommendationMessage", timeOfDay + "에 " + locationText + "좋은 장소들을 추천드립니다."
            );
            
            return new CurrentTimeRecommendationsResponse(placeDtos, timeContext, weatherCondition, timeOfDay);
        } catch (Exception e) {
            // Fallback to basic recommendations on error
            PageRequest pageRequest = PageRequest.of(0, Math.min(limit, 10));
            List<Place> places = placeRepository.findRecommendablePlaces(pageRequest).getContent();
            
            List<SimplePlaceDto> placeDtos = places.stream()
                .map(this::convertToSimplePlaceDto)
                .collect(Collectors.toList());
            
            java.time.LocalTime now = java.time.LocalTime.now();
            String timeOfDay = getTimeOfDay(now);
            Map<String, Object> timeContext = Map.of(
                "timeOfDay", timeOfDay,
                "weatherCondition", "맑음",
                "recommendationMessage", "추천 장소를 불러왔습니다."
            );
            
            return new CurrentTimeRecommendationsResponse(placeDtos, timeContext, "맑음", timeOfDay);
        }
    }
    
    public PlaceListResponse getPlacesList(int page, int limit, String sort) {
        PageRequest pageRequest = PageRequest.of(page, limit);
        Page<Place> placePage;
        
        switch (sort.toLowerCase()) {
            case "popularity":
                placePage = placeRepository.findPopularPlaces(pageRequest);
                break;
            case "rating":
                placePage = placeRepository.findTopRatedPlaces(3.0, pageRequest);
                break;
            case "recent":
            default:
                placePage = placeRepository.findRecommendablePlaces(pageRequest);
                break;
        }
        
        List<SimplePlaceDto> placeDtos = placePage.getContent().stream()
            .map(this::convertToSimplePlaceDto)
            .collect(Collectors.toList());
        
        return new PlaceListResponse(placeDtos, (int) placePage.getTotalElements(), 
                                   placePage.getTotalPages(), page, limit);
    }
    
    /**
     * Get places with images for home page
     */
    public List<PlaceDto.PlaceResponse> getPlacesWithImages(int limit) {
        // Use a simple approach to get places with gallery images
        Pageable pageable = PageRequest.of(0, limit * 3); // Get more to filter
        List<Place> allPlaces = placeRepository.findTopRatedPlaces(3.0, pageable).getContent();
        
        // Filter places that have gallery images and limit the results
        List<Place> places = allPlaces.stream()
            .filter(place -> place.getGallery() != null && !place.getGallery().isEmpty())
            .limit(limit)
            .collect(Collectors.toList());
        
        return places.stream()
            .map(place -> {
                // Get primary image or first available image
                String imageUrl = getPlaceImageUrl(place);
                
                // Get all images for this place from gallery
                List<String> images = place.getGallery() != null && !place.getGallery().isEmpty() ? 
                    place.getGallery() : 
                    List.of();
                
                return new PlaceDto.PlaceResponse(
                    place.getId(),
                    place.getName() != null ? place.getName() : place.getTitle(),
                    imageUrl,
                    images,
                    place.getRating() != null ? place.getRating().doubleValue() : 4.0,
                    place.getCategory() != null ? place.getCategory() : "카테고리 없음"
                );
            })
            .collect(Collectors.toList());
    }
    
    private SimplePlaceDto convertToSimplePlaceDto(Place place) {
        String imageUrl = getPlaceImageUrl(place);
        
        SimplePlaceDto dto = new SimplePlaceDto(
            place.getId().toString(),
            place.getName() != null ? place.getName() : place.getTitle(),
            place.getCategory() != null ? place.getCategory() : "기타",
            place.getRating() != null ? place.getRating().doubleValue() : 4.0,
            place.getAddress(),
            imageUrl
        );
        
        // Set additional fields
        dto.setTitle(place.getTitle());
        dto.setReviewCount(place.getReviewCount() != null ? place.getReviewCount() : 0);
        dto.setAddress(place.getAddress());
        dto.setImages(place.getGallery());
        dto.setDescription(place.getDescription());
        dto.setTags(place.getTags());
        dto.setPhone(place.getPhone());
        dto.setWebsiteUrl(place.getWebsiteUrl());
        dto.setAmenities(place.getAmenities());
        dto.setDistance(0.0); // TODO: Calculate actual distance
        dto.setIsBookmarked(false); // TODO: Check if bookmarked by current user
        dto.setIsDemo(false);
        
        return dto;
    }
    
    private String getPlaceImageUrl(Place place) {
        // Get first image from gallery
        if (place.getGallery() != null && !place.getGallery().isEmpty()) {
            return place.getGallery().get(0);
        }
        
        return null;
    }
    
    private String getTimeOfDay(java.time.LocalTime time) {
        int hour = time.getHour();
        if (hour >= 6 && hour < 12) return "아침";
        if (hour >= 12 && hour < 18) return "오후";
        if (hour >= 18 && hour < 22) return "저녁";
        return "밤";
    }
}
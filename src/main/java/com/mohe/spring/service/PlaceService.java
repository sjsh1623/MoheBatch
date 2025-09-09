package com.mohe.spring.service;

import com.mohe.spring.dto.*;
import com.mohe.spring.entity.Place;
import com.mohe.spring.repository.PlaceRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class PlaceService {
    
    private final PlaceRepository placeRepository;
    public PlaceService(PlaceRepository placeRepository) {
        this.placeRepository = placeRepository;
    }
    
    public PlaceRecommendationsResponse getRecommendations() {
        // TODO: Implement place recommendations logic
        throw new UnsupportedOperationException("Place recommendations not yet implemented");
    }
    
    public PlaceListResponse getPlaces(int page, int limit, String category, String sort) {
        // TODO: Implement place listing logic
        throw new UnsupportedOperationException("Place listing not yet implemented");
    }
    
    public PlaceDetailResponse getPlaceDetail(String id) {
        // TODO: Implement place detail logic
        throw new UnsupportedOperationException("Place detail not yet implemented");
    }
    
    public PlaceSearchResponse searchPlaces(String q, String location, String weather, String time) {
        // TODO: Implement place search logic
        throw new UnsupportedOperationException("Place search not yet implemented");
    }
    
    public Map<String, Object> getDebugInfo() {
        // TODO: Implement debug info logic
        throw new UnsupportedOperationException("Debug info not yet implemented");
    }
    
    public PlaceListResponse getPopularPlaces(double latitude, double longitude) {
        // TODO: Implement popular places logic
        throw new UnsupportedOperationException("Popular places not yet implemented");
    }
    
    public CurrentTimeRecommendationsResponse getCurrentTimePlaces(Double latitude, Double longitude, int limit) {
        // TODO: Implement current time recommendations logic
        throw new UnsupportedOperationException("Current time recommendations not yet implemented");
    }
    
    public PlaceListResponse getPlacesList(int page, int limit, String sort) {
        // TODO: Implement places list logic
        throw new UnsupportedOperationException("Places list not yet implemented");
    }
    
    /**
     * Get places with images for home page
     */
    public List<PlaceDto.PlaceResponse> getPlacesWithImages(int limit) {
        Pageable pageable = PageRequest.of(0, limit);
        
        // Get places with highest ratings that have at least 5 images
        List<Place> places = placeRepository.findTopRatedPlacesWithImages(3.0, pageable).getContent();
        
        return places.stream()
            .map(place -> {
                // Get primary image or first available image
                String imageUrl = getPlaceImageUrl(place);
                
                // Get all images for this place - prioritize images array, fallback to single imageUrl
                List<String> images = place.getImages() != null && !place.getImages().isEmpty() ? 
                    place.getImages() : 
                    (imageUrl != null ? List.of(imageUrl) : List.of());
                
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
    
    private String getPlaceImageUrl(Place place) {
        // First try to get from images array
        if (place.getImages() != null && !place.getImages().isEmpty()) {
            return place.getImages().get(0);
        }
        
        // If no images in array, return the place's imageUrl field as fallback
        return place.getImageUrl();
    }
}
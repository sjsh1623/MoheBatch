package com.mohe.spring.service;

import com.mohe.spring.dto.VectorSimilarityResponse;
import com.mohe.spring.dto.PlaceDto;
import com.mohe.spring.entity.*;
import com.mohe.spring.repository.*;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@Transactional(readOnly = true)
public class VectorSearchService {
    
    private final PlaceDescriptionVectorRepository placeDescriptionVectorRepository;
    private final UserPreferenceVectorRepository userPreferenceVectorRepository;
    private final VectorSimilarityRepository vectorSimilarityRepository;
    private final PlaceRepository placeRepository;
    private final UserRepository userRepository;
    
    public VectorSearchService(PlaceDescriptionVectorRepository placeDescriptionVectorRepository,
                              UserPreferenceVectorRepository userPreferenceVectorRepository,
                              VectorSimilarityRepository vectorSimilarityRepository,
                              PlaceRepository placeRepository,
                              UserRepository userRepository) {
        this.placeDescriptionVectorRepository = placeDescriptionVectorRepository;
        this.userPreferenceVectorRepository = userPreferenceVectorRepository;
        this.vectorSimilarityRepository = vectorSimilarityRepository;
        this.placeRepository = placeRepository;
        this.userRepository = userRepository;
    }
    
    /**
     * Perform vector-based search for places matching a text query
     */
    public List<Place> vectorSearchPlaces(String query, int limit) {
        // For now, use keyword matching until we implement proper query vectorization
        return keywordBasedSearch(query, limit);
    }
    
    /**
     * Get personalized place recommendations using vector similarity
     */
    public List<Place> getPersonalizedRecommendations(String userEmail, int limit) {
        try {
            // Get user from authentication
            Optional<User> userOpt = userRepository.findByEmail(userEmail);
            if (userOpt.isEmpty()) {
                return List.of();
            }
            
            User user = userOpt.get();
            
            // Get user's preference vector
            Optional<UserPreferenceVector> userVectorOpt = userPreferenceVectorRepository.findByUserId(user.getId());
            if (userVectorOpt.isEmpty()) {
                // Fallback to basic recommendations
                return placeRepository.findRecommendablePlaces(PageRequest.of(0, limit)).getContent();
            }
            
            // Get cached vector similarities for this user
            List<VectorSimilarity> similarities = vectorSimilarityRepository
                .findTopSimilarPlaces(user.getId(), 0.3, PageRequest.of(0, limit * 2));
            
            if (!similarities.isEmpty()) {
                // Use cached similarities
                List<Long> placeIds = similarities.stream()
                    .map(VectorSimilarity::getPlaceId)
                    .collect(Collectors.toList());
                
                return placeRepository.findAllById(placeIds).stream()
                    .limit(limit)
                    .collect(Collectors.toList());
            } else {
                // Calculate similarities on-demand
                return calculateVectorSimilarities(user, userVectorOpt.get(), limit);
            }
            
        } catch (Exception e) {
            // Fallback to basic recommendations on error
            return placeRepository.findRecommendablePlaces(PageRequest.of(0, limit)).getContent();
        }
    }
    
    /**
     * Get personalized recommendations for authenticated user
     */
    public List<Place> getPersonalizedRecommendations(int limit) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.isAuthenticated() && !auth.getName().equals("anonymousUser")) {
            return getPersonalizedRecommendations(auth.getName(), limit);
        }
        
        // Fallback for unauthenticated users
        return placeRepository.findRecommendablePlaces(PageRequest.of(0, limit)).getContent();
    }
    
    /**
     * Search places with vector similarity based on user preferences
     */
    public VectorSimilarityResponse searchWithVectorSimilarity(String query, String userEmail, double similarityThreshold, int limit) {
        long startTime = System.currentTimeMillis();
        
        try {
            Optional<User> userOpt = userRepository.findByEmail(userEmail);
            if (userOpt.isEmpty()) {
                return createEmptyResponse(null, null, similarityThreshold, startTime);
            }
            
            User user = userOpt.get();
            Optional<UserPreferenceVector> userVectorOpt = userPreferenceVectorRepository.findByUserId(user.getId());
            if (userVectorOpt.isEmpty()) {
                return createEmptyResponse(user.getId(), null, similarityThreshold, startTime);
            }
            
            UserPreferenceVector userVector = userVectorOpt.get();
            
            // First, do keyword-based search to get candidate places
            List<Place> candidatePlaces = keywordBasedSearch(query, limit * 3);
            
            // Get vector similarities for these places
            List<VectorSimilarityResponse.SimilarPlace> similarPlaces = new ArrayList<>();
            
            for (Place place : candidatePlaces) {
                Optional<PlaceDescriptionVector> placeVectorOpt = placeDescriptionVectorRepository.findByPlaceId(place.getId());
                if (placeVectorOpt.isPresent()) {
                    PlaceDescriptionVector placeVector = placeVectorOpt.get();
                    VectorSimilarityResult result = placeVector.calculateSimilarityWithUser(userVector);
                    
                    if (result.getWeightedSimilarity() >= similarityThreshold) {
                        VectorSimilarityResponse.SimilarPlace similarPlace = new VectorSimilarityResponse.SimilarPlace(
                            convertToPlaceDto(place),
                            result.getWeightedSimilarity(),
                            result.getCosineSimilarity(),
                            result.getWeightedSimilarity(),
                            String.format("벡터 유사도: %.2f, MBTI 가중치: %.2f", 
                                result.getCosineSimilarity(), result.getMbtiBoostFactor())
                        );
                        similarPlaces.add(similarPlace);
                    }
                }
            }
            
            // Sort by weighted similarity
            similarPlaces.sort((a, b) -> Double.compare(b.getWeightedSimilarity(), a.getWeightedSimilarity()));
            
            // Limit results
            List<VectorSimilarityResponse.SimilarPlace> limitedResults = similarPlaces.stream()
                .limit(limit)
                .collect(Collectors.toList());
            
            long searchTime = System.currentTimeMillis() - startTime;
            
            return VectorSimilarityResponse.of(
                limitedResults,
                user.getId(),
                null,
                similarityThreshold,
                "hybrid_vector_keyword",
                searchTime
            );
            
        } catch (Exception e) {
            long searchTime = System.currentTimeMillis() - startTime;
            return createEmptyResponse(null, null, similarityThreshold, searchTime);
        }
    }
    
    /**
     * Calculate vector similarities on-demand
     */
    private List<Place> calculateVectorSimilarities(User user, UserPreferenceVector userVector, int limit) {
        List<PlaceDescriptionVector> allPlaceVectors = placeDescriptionVectorRepository.findAllActive();
        List<SimilarityScore> scores = new ArrayList<>();
        
        for (PlaceDescriptionVector placeVector : allPlaceVectors) {
            try {
                var result = placeVector.calculateSimilarityWithUser(userVector);
                if (result.getWeightedSimilarity() > 0.2) { // Minimum threshold
                    scores.add(new SimilarityScore(placeVector.getPlace(), result.getWeightedSimilarity()));
                }
            } catch (Exception e) {
                // Skip this place on error
                continue;
            }
        }
        
        // Sort by similarity and return top places
        return scores.stream()
            .sorted((a, b) -> Double.compare(b.similarity, a.similarity))
            .map(score -> score.place)
            .limit(limit)
            .collect(Collectors.toList());
    }
    
    /**
     * Keyword-based search as fallback
     */
    private List<Place> keywordBasedSearch(String query, int limit) {
        Pageable pageable = PageRequest.of(0, limit);
        return placeRepository.searchPlaces(query, pageable).getContent();
    }
    
    /**
     * Convert Place entity to PlaceDto.PlaceResponse for API response
     */
    private PlaceDto.PlaceResponse convertToPlaceDto(Place place) {
        return new PlaceDto.PlaceResponse(
            place.getId(),
            place.getName() != null ? place.getName() : place.getTitle(),
            getPlaceImageUrl(place),
            place.getGallery(),
            place.getRating() != null ? place.getRating().doubleValue() : 4.0,
            place.getCategory() != null ? place.getCategory() : "기타"
        );
    }
    
    private String getPlaceImageUrl(Place place) {
        if (place.getGallery() != null && !place.getGallery().isEmpty()) {
            return place.getGallery().get(0);
        }
        return null;
    }
    
    /**
     * Create empty response for error cases
     */
    private VectorSimilarityResponse createEmptyResponse(Long userId, Long queryPlaceId, double similarityThreshold, long startTime) {
        long searchTime = System.currentTimeMillis() - startTime;
        return VectorSimilarityResponse.of(
            List.of(),
            userId,
            queryPlaceId,
            similarityThreshold,
            "fallback",
            searchTime
        );
    }
    
    /**
     * Helper class for sorting places by similarity score
     */
    private static class SimilarityScore {
        final Place place;
        final double similarity;
        
        SimilarityScore(Place place, double similarity) {
            this.place = place;
            this.similarity = similarity;
        }
    }
}
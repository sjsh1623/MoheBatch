package com.mohe.spring.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohe.spring.controller.InternalBatchController;
import com.mohe.spring.entity.Place;
import com.mohe.spring.repository.PlaceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class InternalPlaceIngestService {

    private static final Logger log = LoggerFactory.getLogger(InternalPlaceIngestService.class);
    private final PlaceRepository placeRepository;
    private final ObjectMapper objectMapper;
    
    public InternalPlaceIngestService(PlaceRepository placeRepository, ObjectMapper objectMapper) {
        this.placeRepository = placeRepository;
        this.objectMapper = objectMapper;
    }
    
    @Transactional
    public InternalBatchController.InternalPlaceIngestResponse ingestPlaces(List<InternalBatchController.InternalPlaceIngestRequest> requests) {
        log.info("Starting place ingestion for {} requests", requests.size());
        int processedCount = 0;
        int insertedCount = 0;
        int updatedCount = 0;
        int skippedCount = 0;
        int errorCount = 0;
        List<String> errors = new ArrayList<>();
        
        for (InternalBatchController.InternalPlaceIngestRequest request : requests) {
            try {
                processedCount++;
                log.debug("Processing place {}/{}: {}", processedCount, requests.size(), request.getName());
                log.info("NaverID: " + request.getNaverPlaceId() + ", GoogleID: " + request.getGooglePlaceId());
                
                // Check if place already exists by naverPlaceId or googlePlaceId
                Optional<Place> existingPlace = findExistingPlace(request);
                
                if (existingPlace.isPresent()) {
                    // Update existing place
                    log.info("UPDATING existing place: " + request.getName() + " (DB ID: " + existingPlace.get().getId() + ")");
                    Place place = existingPlace.get();
                    updatePlaceFromRequest(place, request);
                    Place saved = placeRepository.save(place);
                    log.info("Successfully updated place with ID: " + saved.getId());
                    updatedCount++;
                } else {
                    // Create new place
                    log.info("CREATING new place: " + request.getName());
                    Place newPlace = createPlaceFromRequest(request);
                    Place saved = placeRepository.save(newPlace);
                    log.info("Successfully created NEW place with ID: " + saved.getId());
                    insertedCount++;
                }
                
            } catch (org.springframework.dao.DataIntegrityViolationException e) {
                log.info("CONSTRAINT VIOLATION for place: " + request.getName());
                log.info("NaverID: " + request.getNaverPlaceId() + ", GoogleID: " + request.getGooglePlaceId());
                log.info("Error message: " + e.getMessage());
                
                // Try to find existing place one more time with more debug info
                log.info("Attempting secondary lookup for constraint violation...");
                Optional<Place> debugLookup = findExistingPlace(request);
                if (debugLookup.isPresent()) {
                    log.info("FOUND place in secondary lookup - this indicates a race condition or caching issue");
                } else {
                    log.info("Still no place found in secondary lookup - possible data issue");
                }
                
                skippedCount++;
            } catch (Exception e) {
                log.info("ERROR processing place: " + request.getName() + " - " + e.getMessage());
                e.printStackTrace();
                errorCount++;
                errors.add("Failed to process place '" + request.getName() + "': " + e.getMessage());
            }
        }
        
        return new InternalBatchController.InternalPlaceIngestResponse(
            processedCount,
            insertedCount,
            updatedCount,
            skippedCount,
            errorCount,
            0, // keywordGeneratedCount - TODO: implement keyword processing
            0, // imagesFetchedCount - TODO: implement image fetching
            errors
        );
    }
    
    private Optional<Place> findExistingPlace(InternalBatchController.InternalPlaceIngestRequest request) {
        log.info("Checking for existing place - NaverID: " + request.getNaverPlaceId() + ", GoogleID: " + request.getGooglePlaceId());
        
        if (request.getNaverPlaceId() != null && !request.getNaverPlaceId().trim().isEmpty()) {
            Optional<Place> byNaverId = placeRepository.findByNaverPlaceId(request.getNaverPlaceId().trim());
            if (byNaverId.isPresent()) {
                log.info("Found existing place by NaverID: " + byNaverId.get().getId());
                return byNaverId;
            }
        }
        
        if (request.getGooglePlaceId() != null && !request.getGooglePlaceId().trim().isEmpty()) {
            Optional<Place> byGoogleId = placeRepository.findByGooglePlaceId(request.getGooglePlaceId().trim());
            if (byGoogleId.isPresent()) {
                log.info("Found existing place by GoogleID: " + byGoogleId.get().getId());
                return byGoogleId;
            }
        }
        
        log.info("No existing place found for: " + request.getName());
        return Optional.empty();
    }
    
    private Place createPlaceFromRequest(InternalBatchController.InternalPlaceIngestRequest request) {
        Place place = new Place(request.getName());
        updatePlaceFromRequest(place, request);
        return place;
    }
    
    private void updatePlaceFromRequest(Place place, InternalBatchController.InternalPlaceIngestRequest request) {
        try {
            place.setName(request.getName());
            place.setTitle(request.getName()); // For backward compatibility
            place.setDescription(request.getDescription());
            place.setCategory(request.getCategory());
            place.setAddress(request.getAddress());
            place.setRoadAddress(request.getRoadAddress());
            place.setLatitude(request.getLatitude());
            place.setLongitude(request.getLongitude());
            place.setPhone(request.getPhone());
            place.setWebsiteUrl(request.getWebsiteUrl());
            
            if (request.getRating() != null) {
                place.setRating(java.math.BigDecimal.valueOf(request.getRating()));
            }
            
            place.setUserRatingsTotal(request.getUserRatingsTotal());
            
            if (request.getPriceLevel() != null) {
                place.setPriceLevel(request.getPriceLevel().shortValue());
            }
            
            place.setNaverPlaceId(request.getNaverPlaceId());
            place.setGooglePlaceId(request.getGooglePlaceId());
            // Add image to gallery if provided
            if (request.getImageUrl() != null && !request.getImageUrl().trim().isEmpty()) {
                List<String> gallery = place.getGallery();
                if (gallery == null) {
                    gallery = new ArrayList<>();
                    place.setGallery(gallery);
                }
                if (!gallery.contains(request.getImageUrl().trim())) {
                    gallery.add(request.getImageUrl().trim());
                }
            }
            
            if (request.getTypes() != null) {
                place.setTypes(request.getTypes());
            }
            
            // Parse JSON fields
            if (request.getOpeningHours() != null) {
                JsonNode openingHoursJson = objectMapper.readTree(request.getOpeningHours());
                place.setOpeningHours(openingHoursJson);
            }
            
            if (request.getSourceFlags() != null) {
                JsonNode sourceFlagsJson = objectMapper.valueToTree(request.getSourceFlags());
                place.setSourceFlags(sourceFlagsJson);
            }
            
            // Store keyword vector as JSON string
            if (request.getKeywordVector() != null && !request.getKeywordVector().isEmpty()) {
                String vectorJson = objectMapper.writeValueAsString(request.getKeywordVector());
                place.setKeywordVector(vectorJson);
            }
            
            place.setUpdatedAt(java.time.LocalDateTime.now());
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to update place from request", e);
        }
    }
    
    public Object triggerDataIngestion() {
        // TODO: Implement data ingestion trigger
        throw new UnsupportedOperationException("Data ingestion trigger not yet implemented");
    }
    
    public Object getIngestionStatus() {
        // TODO: Implement ingestion status retrieval
        throw new UnsupportedOperationException("Ingestion status retrieval not yet implemented");
    }
    
    public Object triggerSpecificIngestion(String category, int limit) {
        // TODO: Implement specific category ingestion
        throw new UnsupportedOperationException("Specific category ingestion not yet implemented");
    }
}
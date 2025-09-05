package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.dto.ErrorCode;
import com.mohe.spring.service.InternalPlaceIngestService;
import io.swagger.v3.oas.annotations.Operation;
// Import removed - using fully qualified name for ApiResponse to avoid conflict
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Internal endpoints for batch processing
 * These endpoints are used by the MoheBatch service to ingest places
 */
@RestController
@RequestMapping("/api/batch/internal")
@Tag(name = "Internal Batch Processing", description = "Internal endpoints used by MoheBatch service")
public class InternalBatchController {

    private final InternalPlaceIngestService internalPlaceIngestService;

    public InternalBatchController(InternalPlaceIngestService internalPlaceIngestService) {
        this.internalPlaceIngestService = internalPlaceIngestService;
    }

    @PostMapping("/ingest/place")
    @Operation(
        summary = "Internal place ingestion endpoint",
        description = "Used by MoheBatch service to ingest processed places with image fetching"
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Places ingested successfully"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Invalid request data")
        }
    )
    public ResponseEntity<ApiResponse<InternalPlaceIngestResponse>> ingestPlaces(
            @RequestBody List<InternalPlaceIngestRequest> requests,
            HttpServletRequest httpRequest) {
        try {
            InternalPlaceIngestResponse response = internalPlaceIngestService.ingestPlaces(requests);
            return ResponseEntity.ok(ApiResponse.success(response));
        } catch (Exception e) {
            ApiResponse<InternalPlaceIngestResponse> errorResponse = ApiResponse.error(
                ErrorCode.INTERNAL_SERVER_ERROR,
                e.getMessage() != null ? e.getMessage() : "Failed to ingest places",
                httpRequest.getRequestURI()
            );
            return ResponseEntity.badRequest().body(errorResponse);
        }
    }

    /**
     * Request DTOs for internal batch communication
     */
    public static class InternalPlaceIngestRequest {
        private String naverPlaceId;
        private String googlePlaceId;
        private String name;
        private String description;
        private String category;
        private String address;
        private String roadAddress;
        private BigDecimal latitude;
        private BigDecimal longitude;
        private String phone;
        private String websiteUrl;
        private Double rating;
        private Integer userRatingsTotal;
        private Integer priceLevel;
        private List<String> types;
        private String openingHours; // JSON string
        private String imageUrl;
        private Map<String, Object> sourceFlags;
        private String naverRawData; // JSON string
        private String googleRawData; // JSON string
        private List<Double> keywordVector = List.of(); // Embedding vector from Ollama

        public InternalPlaceIngestRequest() {}

        // Getters and setters
        public String getNaverPlaceId() { return naverPlaceId; }
        public void setNaverPlaceId(String naverPlaceId) { this.naverPlaceId = naverPlaceId; }
        
        public String getGooglePlaceId() { return googlePlaceId; }
        public void setGooglePlaceId(String googlePlaceId) { this.googlePlaceId = googlePlaceId; }
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        
        public String getCategory() { return category; }
        public void setCategory(String category) { this.category = category; }
        
        public String getAddress() { return address; }
        public void setAddress(String address) { this.address = address; }
        
        public String getRoadAddress() { return roadAddress; }
        public void setRoadAddress(String roadAddress) { this.roadAddress = roadAddress; }
        
        public BigDecimal getLatitude() { return latitude; }
        public void setLatitude(BigDecimal latitude) { this.latitude = latitude; }
        
        public BigDecimal getLongitude() { return longitude; }
        public void setLongitude(BigDecimal longitude) { this.longitude = longitude; }
        
        public String getPhone() { return phone; }
        public void setPhone(String phone) { this.phone = phone; }
        
        public String getWebsiteUrl() { return websiteUrl; }
        public void setWebsiteUrl(String websiteUrl) { this.websiteUrl = websiteUrl; }
        
        public Double getRating() { return rating; }
        public void setRating(Double rating) { this.rating = rating; }
        
        public Integer getUserRatingsTotal() { return userRatingsTotal; }
        public void setUserRatingsTotal(Integer userRatingsTotal) { this.userRatingsTotal = userRatingsTotal; }
        
        public Integer getPriceLevel() { return priceLevel; }
        public void setPriceLevel(Integer priceLevel) { this.priceLevel = priceLevel; }
        
        public List<String> getTypes() { return types; }
        public void setTypes(List<String> types) { this.types = types; }
        
        public String getOpeningHours() { return openingHours; }
        public void setOpeningHours(String openingHours) { this.openingHours = openingHours; }
        
        public String getImageUrl() { return imageUrl; }
        public void setImageUrl(String imageUrl) { this.imageUrl = imageUrl; }
        
        public Map<String, Object> getSourceFlags() { return sourceFlags; }
        public void setSourceFlags(Map<String, Object> sourceFlags) { this.sourceFlags = sourceFlags; }
        
        public String getNaverRawData() { return naverRawData; }
        public void setNaverRawData(String naverRawData) { this.naverRawData = naverRawData; }
        
        public String getGoogleRawData() { return googleRawData; }
        public void setGoogleRawData(String googleRawData) { this.googleRawData = googleRawData; }
        
        public List<Double> getKeywordVector() { return keywordVector; }
        public void setKeywordVector(List<Double> keywordVector) { this.keywordVector = keywordVector; }
    }

    /**
     * Response DTOs for internal batch communication
     */
    public static class InternalPlaceIngestResponse {
        private final int processedCount;
        private final int insertedCount;
        private final int updatedCount;
        private final int skippedCount;
        private final int errorCount;
        private final int keywordGeneratedCount;
        private final int imagesFetchedCount; // New: count of images fetched
        private final List<String> errors;

        public InternalPlaceIngestResponse(int processedCount, int insertedCount, int updatedCount, int skippedCount,
                                         int errorCount, int keywordGeneratedCount, int imagesFetchedCount, List<String> errors) {
            this.processedCount = processedCount;
            this.insertedCount = insertedCount;
            this.updatedCount = updatedCount;
            this.skippedCount = skippedCount;
            this.errorCount = errorCount;
            this.keywordGeneratedCount = keywordGeneratedCount;
            this.imagesFetchedCount = imagesFetchedCount;
            this.errors = errors;
        }

        public int getProcessedCount() { return processedCount; }
        public int getInsertedCount() { return insertedCount; }
        public int getUpdatedCount() { return updatedCount; }
        public int getSkippedCount() { return skippedCount; }
        public int getErrorCount() { return errorCount; }
        public int getKeywordGeneratedCount() { return keywordGeneratedCount; }
        public int getImagesFetchedCount() { return imagesFetchedCount; }
        public List<String> getErrors() { return errors; }
    }
}
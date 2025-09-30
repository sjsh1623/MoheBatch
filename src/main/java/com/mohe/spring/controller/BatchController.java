package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.dto.ErrorCode;
import com.mohe.spring.service.EnhancedBatchService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
// Import removed - using fully qualified name for ApiResponse to avoid conflict
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/batch")
@Tag(name = "배치 데이터 수집", description = "배치 프로세스를 위한 데이터 수집 API")
public class BatchController {

    private final EnhancedBatchService enhancedBatchService;

    public BatchController(EnhancedBatchService enhancedBatchService) {
        this.enhancedBatchService = enhancedBatchService;
    }

    @PostMapping("/places")
    @Operation(
        summary = "배치 장소 데이터 수집",
        description = "외부 API에서 수집한 장소 데이터를 처리하여 저장합니다."
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "배치 데이터 수집 성공",
                content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = BatchPlaceResponse.class)
                )
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "400",
                description = "잘못된 요청 데이터"
            )
        }
    )
    public ResponseEntity<ApiResponse<BatchPlaceResponse>> ingestPlaceData(
            @Parameter(description = "장소 데이터 배열", required = true)
            @Valid @RequestBody List<BatchPlaceRequest> placeDataList,
            HttpServletRequest httpRequest) {
        try {
            BatchPlaceResponse response = enhancedBatchService.ingestPlaceData(placeDataList);
            return ResponseEntity.ok(ApiResponse.success(response));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    ErrorCode.VALIDATION_ERROR,
                    e.getMessage() != null ? e.getMessage() : "잘못된 장소 데이터입니다",
                    httpRequest.getRequestURI()
                )
            );
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    ErrorCode.INTERNAL_SERVER_ERROR,
                    "장소 데이터 수집 중 오류가 발생했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }

    @PostMapping("/users")
    @Operation(
        summary = "배치 사용자 데이터 수집",
        description = "외부 API에서 수집한 사용자 데이터를 처리하여 저장합니다."
    )
    public ResponseEntity<ApiResponse<BatchUserResponse>> ingestUserData(
            @Parameter(description = "사용자 데이터 배열", required = true)
            @Valid @RequestBody List<BatchUserRequest> userDataList,
            HttpServletRequest httpRequest) {
        try {
            BatchUserResponse response = enhancedBatchService.ingestUserData(userDataList);
            return ResponseEntity.ok(ApiResponse.success(response));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    ErrorCode.VALIDATION_ERROR,
                    e.getMessage() != null ? e.getMessage() : "잘못된 사용자 데이터입니다",
                    httpRequest.getRequestURI()
                )
            );
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    ErrorCode.INTERNAL_SERVER_ERROR,
                    "사용자 데이터 수집 중 오류가 발생했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }


    @PostMapping("/internal/cleanup")
    @Operation(
        summary = "데이터베이스 정리",
        description = "오래되고 평점이 낮은 장소 데이터를 정리합니다."
    )
    public ResponseEntity<ApiResponse<DatabaseCleanupResponse>> cleanupDatabase(
            HttpServletRequest httpRequest) {
        try {
            DatabaseCleanupResponse response = enhancedBatchService.cleanupOldAndLowRatedPlaces();
            return ResponseEntity.ok(ApiResponse.success(response));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    ErrorCode.INTERNAL_SERVER_ERROR,
                    "데이터베이스 정리 중 오류가 발생했습니다: " + e.getMessage(),
                    httpRequest.getRequestURI()
                )
            );
        }
    }

    // DTOs for batch processing
    public static class BatchPlaceRequest {
        private String externalId;
        private String name;
        private String description;
        private String category;
        private String address;
        private Double latitude;
        private Double longitude;
        private String phone;
        private String website;
        private Double rating;
        private List<String> tags;
        private String hours;
        private LocalDateTime externalCreatedAt;
        private LocalDateTime externalUpdatedAt;

        public BatchPlaceRequest() {}

        // Getters and setters
        public String getExternalId() { return externalId; }
        public void setExternalId(String externalId) { this.externalId = externalId; }
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        
        public String getCategory() { return category; }
        public void setCategory(String category) { this.category = category; }
        
        public String getAddress() { return address; }
        public void setAddress(String address) { this.address = address; }
        
        public Double getLatitude() { return latitude; }
        public void setLatitude(Double latitude) { this.latitude = latitude; }
        
        public Double getLongitude() { return longitude; }
        public void setLongitude(Double longitude) { this.longitude = longitude; }
        
        public String getPhone() { return phone; }
        public void setPhone(String phone) { this.phone = phone; }
        
        public String getWebsite() { return website; }
        public void setWebsite(String website) { this.website = website; }
        
        public Double getRating() { return rating; }
        public void setRating(Double rating) { this.rating = rating; }
        
        public List<String> getTags() { return tags; }
        public void setTags(List<String> tags) { this.tags = tags; }
        
        public String getHours() { return hours; }
        public void setHours(String hours) { this.hours = hours; }
        
        public LocalDateTime getExternalCreatedAt() { return externalCreatedAt; }
        public void setExternalCreatedAt(LocalDateTime externalCreatedAt) { this.externalCreatedAt = externalCreatedAt; }
        
        public LocalDateTime getExternalUpdatedAt() { return externalUpdatedAt; }
        public void setExternalUpdatedAt(LocalDateTime externalUpdatedAt) { this.externalUpdatedAt = externalUpdatedAt; }
    }

    public static class BatchUserRequest {
        private String externalId;
        private String name;
        private String email;
        private String phone;
        private String department;
        private String status;
        private LocalDateTime externalCreatedAt;
        private LocalDateTime externalUpdatedAt;

        public BatchUserRequest() {}

        // Getters and setters
        public String getExternalId() { return externalId; }
        public void setExternalId(String externalId) { this.externalId = externalId; }
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
        
        public String getPhone() { return phone; }
        public void setPhone(String phone) { this.phone = phone; }
        
        public String getDepartment() { return department; }
        public void setDepartment(String department) { this.department = department; }
        
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        
        public LocalDateTime getExternalCreatedAt() { return externalCreatedAt; }
        public void setExternalCreatedAt(LocalDateTime externalCreatedAt) { this.externalCreatedAt = externalCreatedAt; }
        
        public LocalDateTime getExternalUpdatedAt() { return externalUpdatedAt; }
        public void setExternalUpdatedAt(LocalDateTime externalUpdatedAt) { this.externalUpdatedAt = externalUpdatedAt; }
    }

    public static class BatchPlaceResponse {
        private final int processedCount;
        private final int insertedCount;
        private final int updatedCount;
        private final int skippedCount;
        private final int errorCount;
        private final List<String> errors;

        public BatchPlaceResponse(int processedCount, int insertedCount, int updatedCount, int skippedCount, int errorCount, List<String> errors) {
            this.processedCount = processedCount;
            this.insertedCount = insertedCount;
            this.updatedCount = updatedCount;
            this.skippedCount = skippedCount;
            this.errorCount = errorCount;
            this.errors = errors;
        }

        public int getProcessedCount() { return processedCount; }
        public int getInsertedCount() { return insertedCount; }
        public int getUpdatedCount() { return updatedCount; }
        public int getSkippedCount() { return skippedCount; }
        public int getErrorCount() { return errorCount; }
        public List<String> getErrors() { return errors; }
    }

    public static class BatchUserResponse {
        private final int processedCount;
        private final int insertedCount;
        private final int updatedCount;
        private final int skippedCount;
        private final int errorCount;
        private final List<String> errors;

        public BatchUserResponse(int processedCount, int insertedCount, int updatedCount, int skippedCount, int errorCount, List<String> errors) {
            this.processedCount = processedCount;
            this.insertedCount = insertedCount;
            this.updatedCount = updatedCount;
            this.skippedCount = skippedCount;
            this.errorCount = errorCount;
            this.errors = errors;
        }

        public int getProcessedCount() { return processedCount; }
        public int getInsertedCount() { return insertedCount; }
        public int getUpdatedCount() { return updatedCount; }
        public int getSkippedCount() { return skippedCount; }
        public int getErrorCount() { return errorCount; }
        public List<String> getErrors() { return errors; }
    }

    // New DTOs for internal place ingestion from batch
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

    public static class InternalPlaceIngestResponse {
        private final int processedCount;
        private final int insertedCount;
        private final int updatedCount;
        private final int skippedCount;
        private final int errorCount;
        private final int keywordGeneratedCount;
        private final List<String> errors;

        public InternalPlaceIngestResponse(int processedCount, int insertedCount, int updatedCount, int skippedCount, int errorCount, int keywordGeneratedCount, List<String> errors) {
            this.processedCount = processedCount;
            this.insertedCount = insertedCount;
            this.updatedCount = updatedCount;
            this.skippedCount = skippedCount;
            this.errorCount = errorCount;
            this.keywordGeneratedCount = keywordGeneratedCount;
            this.errors = errors;
        }

        public int getProcessedCount() { return processedCount; }
        public int getInsertedCount() { return insertedCount; }
        public int getUpdatedCount() { return updatedCount; }
        public int getSkippedCount() { return skippedCount; }
        public int getErrorCount() { return errorCount; }
        public int getKeywordGeneratedCount() { return keywordGeneratedCount; }
        public List<String> getErrors() { return errors; }
    }

    public static class DatabaseCleanupResponse {
        private final int removedCount;
        private final List<String> messages;

        public DatabaseCleanupResponse(int removedCount, List<String> messages) {
            this.removedCount = removedCount;
            this.messages = messages;
        }

        public int getRemovedCount() { return removedCount; }
        public List<String> getMessages() { return messages; }
    }
}
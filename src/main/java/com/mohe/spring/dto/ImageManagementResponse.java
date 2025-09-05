package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class ImageManagementResponse {
    
    @JsonProperty("operation")
    private String operation;
    
    @JsonProperty("place_id")
    private Long placeId;
    
    @JsonProperty("image_urls")
    private List<String> imageUrls;
    
    @JsonProperty("thumbnail_urls")
    private List<String> thumbnailUrls;
    
    @JsonProperty("total_images")
    private Integer totalImages;
    
    @JsonProperty("processing_time_ms")
    private Long processingTimeMs;
    
    @JsonProperty("status")
    private String status;
    
    @JsonProperty("errors")
    private List<String> errors;
    
    @JsonProperty("metadata")
    private ImageMetadata metadata;
    
    // Default constructor
    public ImageManagementResponse() {}
    
    // Constructor with fields
    public ImageManagementResponse(String operation, Long placeId, List<String> imageUrls,
                                  List<String> thumbnailUrls, Integer totalImages,
                                  Long processingTimeMs, String status, List<String> errors,
                                  ImageMetadata metadata) {
        this.operation = operation;
        this.placeId = placeId;
        this.imageUrls = imageUrls;
        this.thumbnailUrls = thumbnailUrls;
        this.totalImages = totalImages;
        this.processingTimeMs = processingTimeMs;
        this.status = status;
        this.errors = errors;
        this.metadata = metadata;
    }
    
    // Static factory methods
    public static ImageManagementResponse uploadSuccess(Long placeId, List<String> imageUrls,
                                                       List<String> thumbnailUrls, Long processingTime) {
        return new ImageManagementResponse("UPLOAD", placeId, imageUrls, thumbnailUrls,
                                         imageUrls.size(), processingTime, "SUCCESS", null, null);
    }
    
    public static ImageManagementResponse deleteSuccess(Long placeId, Integer deletedCount) {
        return new ImageManagementResponse("DELETE", placeId, null, null, deletedCount, 0L, "SUCCESS", null, null);
    }
    
    public static ImageManagementResponse error(String operation, Long placeId, List<String> errors) {
        return new ImageManagementResponse(operation, placeId, null, null, 0, 0L, "ERROR", errors, null);
    }
    
    // Nested class for metadata
    public static class ImageMetadata {
        @JsonProperty("total_size_bytes")
        private Long totalSizeBytes;
        
        @JsonProperty("compression_ratio")
        private Double compressionRatio;
        
        @JsonProperty("formats_generated")
        private List<String> formatsGenerated;
        
        // Default constructor
        public ImageMetadata() {}
        
        // Constructor
        public ImageMetadata(Long totalSizeBytes, Double compressionRatio, List<String> formatsGenerated) {
            this.totalSizeBytes = totalSizeBytes;
            this.compressionRatio = compressionRatio;
            this.formatsGenerated = formatsGenerated;
        }
        
        // Getters and setters
        public Long getTotalSizeBytes() { return totalSizeBytes; }
        public void setTotalSizeBytes(Long totalSizeBytes) { this.totalSizeBytes = totalSizeBytes; }
        public Double getCompressionRatio() { return compressionRatio; }
        public void setCompressionRatio(Double compressionRatio) { this.compressionRatio = compressionRatio; }
        public List<String> getFormatsGenerated() { return formatsGenerated; }
        public void setFormatsGenerated(List<String> formatsGenerated) { this.formatsGenerated = formatsGenerated; }
    }
    
    // Getters and setters
    public String getOperation() {
        return operation;
    }
    
    public void setOperation(String operation) {
        this.operation = operation;
    }
    
    public Long getPlaceId() {
        return placeId;
    }
    
    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }
    
    public List<String> getImageUrls() {
        return imageUrls;
    }
    
    public void setImageUrls(List<String> imageUrls) {
        this.imageUrls = imageUrls;
    }
    
    public List<String> getThumbnailUrls() {
        return thumbnailUrls;
    }
    
    public void setThumbnailUrls(List<String> thumbnailUrls) {
        this.thumbnailUrls = thumbnailUrls;
    }
    
    public Integer getTotalImages() {
        return totalImages;
    }
    
    public void setTotalImages(Integer totalImages) {
        this.totalImages = totalImages;
    }
    
    public Long getProcessingTimeMs() {
        return processingTimeMs;
    }
    
    public void setProcessingTimeMs(Long processingTimeMs) {
        this.processingTimeMs = processingTimeMs;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public List<String> getErrors() {
        return errors;
    }
    
    public void setErrors(List<String> errors) {
        this.errors = errors;
    }
    
    public ImageMetadata getMetadata() {
        return metadata;
    }
    
    public void setMetadata(ImageMetadata metadata) {
        this.metadata = metadata;
    }
}
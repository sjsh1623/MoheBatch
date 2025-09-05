package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class ImageUploadRequest {
    
    @JsonProperty("place_id")
    @NotNull(message = "장소 ID는 필수입니다")
    private Long placeId;
    
    @JsonProperty("image_type")
    private String imageType = "GENERAL"; // GENERAL, THUMBNAIL, INTERIOR, EXTERIOR, MENU
    
    @JsonProperty("source")
    private String source = "USER_UPLOAD"; // USER_UPLOAD, EXTERNAL_API, SYSTEM_GENERATED
    
    @JsonProperty("alt_text")
    private String altText;
    
    @JsonProperty("compress")
    private Boolean compress = true;
    
    @JsonProperty("generate_thumbnail")
    private Boolean generateThumbnail = true;
    
    @JsonProperty("max_width")
    private Integer maxWidth = 1920;
    
    @JsonProperty("max_height")
    private Integer maxHeight = 1080;
    
    // Default constructor
    public ImageUploadRequest() {}
    
    // Constructor with fields
    public ImageUploadRequest(Long placeId, String imageType, String source, String altText,
                             Boolean compress, Boolean generateThumbnail, Integer maxWidth, Integer maxHeight) {
        this.placeId = placeId;
        this.imageType = imageType;
        this.source = source;
        this.altText = altText;
        this.compress = compress;
        this.generateThumbnail = generateThumbnail;
        this.maxWidth = maxWidth;
        this.maxHeight = maxHeight;
    }
    
    // Getters and setters
    public Long getPlaceId() {
        return placeId;
    }
    
    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }
    
    public String getImageType() {
        return imageType;
    }
    
    public void setImageType(String imageType) {
        this.imageType = imageType;
    }
    
    public String getSource() {
        return source;
    }
    
    public void setSource(String source) {
        this.source = source;
    }
    
    public String getAltText() {
        return altText;
    }
    
    public void setAltText(String altText) {
        this.altText = altText;
    }
    
    public Boolean getCompress() {
        return compress;
    }
    
    public void setCompress(Boolean compress) {
        this.compress = compress;
    }
    
    public Boolean getGenerateThumbnail() {
        return generateThumbnail;
    }
    
    public void setGenerateThumbnail(Boolean generateThumbnail) {
        this.generateThumbnail = generateThumbnail;
    }
    
    public Integer getMaxWidth() {
        return maxWidth;
    }
    
    public void setMaxWidth(Integer maxWidth) {
        this.maxWidth = maxWidth;
    }
    
    public Integer getMaxHeight() {
        return maxHeight;
    }
    
    public void setMaxHeight(Integer maxHeight) {
        this.maxHeight = maxHeight;
    }
}
package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "place_images",
       indexes = {
           @Index(name = "idx_place_image_place_id", columnList = "place_id"),
           @Index(name = "idx_place_image_is_primary", columnList = "is_primary"),
           @Index(name = "idx_place_image_display_order", columnList = "display_order")
       })
public class PlaceImage {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", nullable = false)
    private Place place;
    
    @Column(name = "image_url", nullable = false, length = 2048)
    private String imageUrl;
    
    @Column(name = "image_type")
    @Enumerated(EnumType.STRING)
    private ImageType imageType = ImageType.GENERAL;
    
    @Column(name = "is_primary")
    private Boolean isPrimary = false;
    
    @Column(name = "display_order")
    private Integer displayOrder = 0;
    
    @Column(name = "source")
    @Enumerated(EnumType.STRING)
    private ImageSource source = ImageSource.GOOGLE_IMAGES;
    
    @Column(name = "source_id")
    private String sourceId; // Original ID from source API
    
    @Column(name = "width")
    private Integer width;
    
    @Column(name = "height")
    private Integer height;
    
    @Column(name = "file_size")
    private Long fileSize; // in bytes
    
    @Column(name = "alt_text")
    private String altText;
    
    @Column(name = "caption")
    private String caption;
    
    @Column(name = "is_verified")
    private Boolean isVerified = false; // Manual verification flag
    
    @Column(name = "created_at")
    private OffsetDateTime createdAt = OffsetDateTime.now();
    
    @Column(name = "updated_at")
    private OffsetDateTime updatedAt = OffsetDateTime.now();
    
    // Default constructor for JPA
    public PlaceImage() {}
    
    // Constructor with required fields
    public PlaceImage(Place place, String imageUrl) {
        this.place = place;
        this.imageUrl = imageUrl;
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public Place getPlace() {
        return place;
    }
    
    public void setPlace(Place place) {
        this.place = place;
    }
    
    public String getImageUrl() {
        return imageUrl;
    }
    
    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }
    
    public ImageType getImageType() {
        return imageType;
    }
    
    public void setImageType(ImageType imageType) {
        this.imageType = imageType;
    }
    
    public Boolean getIsPrimary() {
        return isPrimary;
    }
    
    public void setIsPrimary(Boolean isPrimary) {
        this.isPrimary = isPrimary;
    }
    
    public Integer getDisplayOrder() {
        return displayOrder;
    }
    
    public void setDisplayOrder(Integer displayOrder) {
        this.displayOrder = displayOrder;
    }
    
    public ImageSource getSource() {
        return source;
    }
    
    public void setSource(ImageSource source) {
        this.source = source;
    }
    
    public String getSourceId() {
        return sourceId;
    }
    
    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }
    
    public Integer getWidth() {
        return width;
    }
    
    public void setWidth(Integer width) {
        this.width = width;
    }
    
    public Integer getHeight() {
        return height;
    }
    
    public void setHeight(Integer height) {
        this.height = height;
    }
    
    public Long getFileSize() {
        return fileSize;
    }
    
    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }
    
    public String getAltText() {
        return altText;
    }
    
    public void setAltText(String altText) {
        this.altText = altText;
    }
    
    public String getCaption() {
        return caption;
    }
    
    public void setCaption(String caption) {
        this.caption = caption;
    }
    
    public Boolean getIsVerified() {
        return isVerified;
    }
    
    public void setIsVerified(Boolean isVerified) {
        this.isVerified = isVerified;
    }
    
    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(OffsetDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
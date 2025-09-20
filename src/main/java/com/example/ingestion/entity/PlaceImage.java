package com.example.ingestion.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.ZonedDateTime;

/**
 * PlaceImage Entity - 기존 Spring 구조와 동일하게 매핑
 * MoheSpring의 place_images 테이블과 호환
 */
@Entity
@Table(name = "place_images")
public class PlaceImage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", nullable = false)
    private Place place;

    @Column(name = "image_url", nullable = false, length = 2048)
    private String imageUrl;

    @Enumerated(EnumType.STRING)
    @Column(name = "image_type", nullable = false, length = 50)
    private ImageType imageType = ImageType.GENERAL;

    @Column(name = "is_primary", nullable = false)
    private Boolean isPrimary = false;

    @Column(name = "display_order", nullable = false)
    private Integer displayOrder = 0;

    @Enumerated(EnumType.STRING)
    @Column(name = "source", nullable = false, length = 50)
    private ImageSource source = ImageSource.GOOGLE_IMAGES;

    @Column(name = "source_id")
    private String sourceId;

    @Column(name = "width")
    private Integer width;

    @Column(name = "height")
    private Integer height;

    @Column(name = "file_size")
    private Long fileSize;

    @Column(name = "alt_text", columnDefinition = "TEXT")
    private String altText;

    @Column(name = "caption", columnDefinition = "TEXT")
    private String caption;

    @Column(name = "is_verified", nullable = false)
    private Boolean isVerified = false;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private ZonedDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private ZonedDateTime updatedAt;

    // Enums
    public enum ImageType {
        GENERAL, EXTERIOR, INTERIOR, FOOD, MENU, AMBIANCE, DETAIL, PANORAMIC
    }

    public enum ImageSource {
        GOOGLE_IMAGES, GOOGLE_PLACES, NAVER, MANUAL_UPLOAD, WEB_SCRAPING
    }

    // Constructors
    public PlaceImage() {}

    public PlaceImage(String imageUrl, ImageType imageType) {
        this.imageUrl = imageUrl;
        this.imageType = imageType;
    }

    public PlaceImage(String imageUrl, ImageType imageType, ImageSource source) {
        this.imageUrl = imageUrl;
        this.imageType = imageType;
        this.source = source;
    }

    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public Place getPlace() { return place; }
    public void setPlace(Place place) { this.place = place; }

    public String getImageUrl() { return imageUrl; }
    public void setImageUrl(String imageUrl) { this.imageUrl = imageUrl; }

    public ImageType getImageType() { return imageType; }
    public void setImageType(ImageType imageType) { this.imageType = imageType; }

    public Boolean getIsPrimary() { return isPrimary; }
    public void setIsPrimary(Boolean isPrimary) { this.isPrimary = isPrimary; }

    public Integer getDisplayOrder() { return displayOrder; }
    public void setDisplayOrder(Integer displayOrder) { this.displayOrder = displayOrder; }

    public ImageSource getSource() { return source; }
    public void setSource(ImageSource source) { this.source = source; }

    public String getSourceId() { return sourceId; }
    public void setSourceId(String sourceId) { this.sourceId = sourceId; }

    public Integer getWidth() { return width; }
    public void setWidth(Integer width) { this.width = width; }

    public Integer getHeight() { return height; }
    public void setHeight(Integer height) { this.height = height; }

    public Long getFileSize() { return fileSize; }
    public void setFileSize(Long fileSize) { this.fileSize = fileSize; }

    public String getAltText() { return altText; }
    public void setAltText(String altText) { this.altText = altText; }

    public String getCaption() { return caption; }
    public void setCaption(String caption) { this.caption = caption; }

    public Boolean getIsVerified() { return isVerified; }
    public void setIsVerified(Boolean isVerified) { this.isVerified = isVerified; }

    public ZonedDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(ZonedDateTime createdAt) { this.createdAt = createdAt; }

    public ZonedDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(ZonedDateTime updatedAt) { this.updatedAt = updatedAt; }

    @Override
    public String toString() {
        return String.format("PlaceImage{id=%d, imageUrl='%s', type=%s, isPrimary=%s}",
                id, imageUrl, imageType, isPrimary);
    }

    // Builder class
    public static class Builder {
        private final PlaceImage placeImage = new PlaceImage();

        public Builder place(Place place) {
            placeImage.setPlace(place);
            return this;
        }

        public Builder imageUrl(String imageUrl) {
            placeImage.setImageUrl(imageUrl);
            return this;
        }

        public Builder imageType(ImageType imageType) {
            placeImage.setImageType(imageType);
            return this;
        }

        public Builder isPrimary(Boolean isPrimary) {
            placeImage.setIsPrimary(isPrimary);
            return this;
        }

        public Builder displayOrder(Integer displayOrder) {
            placeImage.setDisplayOrder(displayOrder);
            return this;
        }

        public Builder source(ImageSource source) {
            placeImage.setSource(source);
            return this;
        }

        public Builder sourceId(String sourceId) {
            placeImage.setSourceId(sourceId);
            return this;
        }

        public Builder width(Integer width) {
            placeImage.setWidth(width);
            return this;
        }

        public Builder height(Integer height) {
            placeImage.setHeight(height);
            return this;
        }

        public Builder fileSize(Long fileSize) {
            placeImage.setFileSize(fileSize);
            return this;
        }

        public Builder altText(String altText) {
            placeImage.setAltText(altText);
            return this;
        }

        public Builder caption(String caption) {
            placeImage.setCaption(caption);
            return this;
        }

        public Builder isVerified(Boolean isVerified) {
            placeImage.setIsVerified(isVerified);
            return this;
        }

        public PlaceImage build() {
            return placeImage;
        }
    }
}
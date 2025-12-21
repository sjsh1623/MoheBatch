package com.mohe.batch.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Table(name = "place_menus")
@Getter
@Setter
@NoArgsConstructor
public class PlaceMenu {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", nullable = false)
    private Place place;

    @Column(name = "name", nullable = false, length = 255)
    private String name;

    @Column(name = "price", length = 50)
    private String price;

    @Column(name = "description", length = 1000)
    private String description;

    @Column(name = "image_url", length = 2048)
    private String imageUrl;

    @Column(name = "image_path", length = 2048)
    private String imagePath;

    @Column(name = "is_popular")
    private Boolean isPopular = false;

    @Column(name = "display_order")
    private Integer displayOrder = 0;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public PlaceMenu(Place place, String name, String price) {
        this.place = place;
        this.name = name;
        this.price = price;
    }

    public boolean hasImage() {
        return imageUrl != null && !imageUrl.isEmpty();
    }

    public boolean isImageDownloaded() {
        return imagePath != null && !imagePath.isEmpty();
    }
}

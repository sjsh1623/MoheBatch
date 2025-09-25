package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.io.Serializable;
import java.util.Objects;

@Entity
@Table(name = "place_similarity",
       indexes = {
           @Index(name = "idx_place_similarity_place1", columnList = "place_id1"),
           @Index(name = "idx_place_similarity_place2", columnList = "place_id2"),
           @Index(name = "idx_place_similarity_jaccard", columnList = "jaccard"),
           @Index(name = "idx_place_similarity_cosine", columnList = "cosine_bin")
       })
@IdClass(PlaceSimilarityId.class)
public class PlaceSimilarity {
    
    @Id
    @Column(name = "place_id1", nullable = false)
    private Long placeId1;
    
    @Id
    @Column(name = "place_id2", nullable = false)
    private Long placeId2;
    
    @Column(precision = 5, scale = 4)
    private BigDecimal jaccard = BigDecimal.ZERO;
    
    @Column(name = "cosine_bin", precision = 5, scale = 4)
    private BigDecimal cosineBin = BigDecimal.ZERO;
    
    @Column(name = "co_users")
    private Integer coUsers = 0;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt = LocalDateTime.now();
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id1", insertable = false, updatable = false)
    private Place place1;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id2", insertable = false, updatable = false)
    private Place place2;
    
    // Default constructor for JPA
    public PlaceSimilarity() {}
    
    // Constructor with required fields
    public PlaceSimilarity(Long placeId1, Long placeId2) {
        this.placeId1 = placeId1;
        this.placeId2 = placeId2;
    }
    
    // Getters and Setters
    public Long getPlaceId1() {
        return placeId1;
    }
    
    public void setPlaceId1(Long placeId1) {
        this.placeId1 = placeId1;
    }
    
    public Long getPlaceId2() {
        return placeId2;
    }
    
    public void setPlaceId2(Long placeId2) {
        this.placeId2 = placeId2;
    }
    
    public BigDecimal getJaccard() {
        return jaccard;
    }
    
    public void setJaccard(BigDecimal jaccard) {
        this.jaccard = jaccard;
    }
    
    public BigDecimal getCosineBin() {
        return cosineBin;
    }
    
    public void setCosineBin(BigDecimal cosineBin) {
        this.cosineBin = cosineBin;
    }
    
    public Integer getCoUsers() {
        return coUsers;
    }
    
    public void setCoUsers(Integer coUsers) {
        this.coUsers = coUsers;
    }
    
    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
    
    public Place getPlace1() {
        return place1;
    }
    
    public void setPlace1(Place place1) {
        this.place1 = place1;
    }
    
    public Place getPlace2() {
        return place2;
    }
    
    public void setPlace2(Place place2) {
        this.place2 = place2;
    }
}
package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "place_similarity_topk",
       indexes = {
           @Index(name = "idx_place_similarity_topk_place", columnList = "place_id, rank"),
           @Index(name = "idx_place_similarity_topk_neighbor", columnList = "neighbor_place_id")
       },
       uniqueConstraints = @UniqueConstraint(columnNames = {"place_id", "rank"}))
@IdClass(PlaceSimilarityTopKId.class)
public class PlaceSimilarityTopK {
    
    @Id
    @Column(name = "place_id", nullable = false)
    private Long placeId;
    
    @Id
    @Column(name = "neighbor_place_id", nullable = false)
    private Long neighborPlaceId;
    
    @Column(nullable = false)
    private Short rank;
    
    @Column(precision = 5, scale = 4)
    private BigDecimal jaccard = BigDecimal.ZERO;
    
    @Column(name = "cosine_bin", precision = 5, scale = 4)
    private BigDecimal cosineBin = BigDecimal.ZERO;
    
    @Column(name = "co_users")
    private Integer coUsers = 0;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt = LocalDateTime.now();
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", insertable = false, updatable = false)
    private Place place;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "neighbor_place_id", insertable = false, updatable = false)
    private Place neighborPlace;
    
    // Default constructor for JPA
    public PlaceSimilarityTopK() {}
    
    // Constructor with required fields
    public PlaceSimilarityTopK(Long placeId, Long neighborPlaceId, Short rank) {
        this.placeId = placeId;
        this.neighborPlaceId = neighborPlaceId;
        this.rank = rank;
    }
    
    // Getters and Setters
    public Long getPlaceId() {
        return placeId;
    }
    
    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }
    
    public Long getNeighborPlaceId() {
        return neighborPlaceId;
    }
    
    public void setNeighborPlaceId(Long neighborPlaceId) {
        this.neighborPlaceId = neighborPlaceId;
    }
    
    public Short getRank() {
        return rank;
    }
    
    public void setRank(Short rank) {
        this.rank = rank;
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
    
    public Place getPlace() {
        return place;
    }
    
    public void setPlace(Place place) {
        this.place = place;
    }
    
    public Place getNeighborPlace() {
        return neighborPlace;
    }
    
    public void setNeighborPlace(Place neighborPlace) {
        this.neighborPlace = neighborPlace;
    }
}
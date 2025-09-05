package com.mohe.spring.entity;

import java.io.Serializable;
import java.util.Objects;

/**
 * Composite primary key for VectorSimilarity
 */
public class VectorSimilarityId implements Serializable {
    private Long userId = 0L;
    private Long placeId = 0L;
    
    public VectorSimilarityId() {}
    
    public VectorSimilarityId(Long userId, Long placeId) {
        this.userId = userId;
        this.placeId = placeId;
    }
    
    public Long getUserId() {
        return userId;
    }
    
    public void setUserId(Long userId) {
        this.userId = userId;
    }
    
    public Long getPlaceId() {
        return placeId;
    }
    
    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorSimilarityId that = (VectorSimilarityId) o;
        return Objects.equals(userId, that.userId) && Objects.equals(placeId, that.placeId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(userId, placeId);
    }
}
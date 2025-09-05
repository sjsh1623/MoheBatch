package com.mohe.spring.entity;

import jakarta.persistence.Embeddable;
import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class PlaceSimilarityId implements Serializable {
    private Long placeId1 = 0L;
    private Long placeId2 = 0L;
    
    public PlaceSimilarityId() {}
    
    public PlaceSimilarityId(Long placeId1, Long placeId2) {
        this.placeId1 = placeId1;
        this.placeId2 = placeId2;
    }
    
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
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlaceSimilarityId that = (PlaceSimilarityId) o;
        return Objects.equals(placeId1, that.placeId1) && Objects.equals(placeId2, that.placeId2);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(placeId1, placeId2);
    }
}
package com.mohe.spring.entity;

import jakarta.persistence.Embeddable;
import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class PlaceSimilarityTopKId implements Serializable {
    private Long placeId = 0L;
    private Long neighborPlaceId = 0L;
    
    public PlaceSimilarityTopKId() {}
    
    public PlaceSimilarityTopKId(Long placeId, Long neighborPlaceId) {
        this.placeId = placeId;
        this.neighborPlaceId = neighborPlaceId;
    }
    
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
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlaceSimilarityTopKId that = (PlaceSimilarityTopKId) o;
        return Objects.equals(placeId, that.placeId) && Objects.equals(neighborPlaceId, that.neighborPlaceId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(placeId, neighborPlaceId);
    }
}
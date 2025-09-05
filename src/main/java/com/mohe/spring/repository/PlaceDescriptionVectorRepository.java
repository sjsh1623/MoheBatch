package com.mohe.spring.repository;

import com.mohe.spring.entity.PlaceDescriptionVector;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PlaceDescriptionVectorRepository extends JpaRepository<PlaceDescriptionVector, Long> {
    
    Optional<PlaceDescriptionVector> findByPlaceId(Long placeId);
    
    void deleteByPlaceId(Long placeId);
    
    List<PlaceDescriptionVector> findByPlaceIdIn(List<Long> placeIds);
    
    @Query("SELECT pdv FROM PlaceDescriptionVector pdv WHERE pdv.place.shouldRecheckRating = false")
    List<PlaceDescriptionVector> findAllActive();
}
package com.mohe.spring.repository

import com.mohe.spring.entity.PlaceDescriptionVector
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository

@Repository
interface PlaceDescriptionVectorRepository : JpaRepository<PlaceDescriptionVector, Long> {
    fun findByPlaceId(placeId: Long): PlaceDescriptionVector?
    fun deleteByPlaceId(placeId: Long)
    fun findByPlaceIdIn(placeIds: List<Long>): List<PlaceDescriptionVector>
    
    @Query("SELECT pdv FROM PlaceDescriptionVector pdv WHERE pdv.place.shouldRecheckRating = false")
    fun findAllActive(): List<PlaceDescriptionVector>
}
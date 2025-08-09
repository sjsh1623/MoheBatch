package com.mohe.spring.repository

import com.mohe.spring.entity.RecentView
import com.mohe.spring.entity.User
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.time.OffsetDateTime

@Repository
interface RecentViewRepository : JpaRepository<RecentView, Long> {
    
    fun findByUserOrderByViewedAtDesc(user: User, pageable: Pageable): Page<RecentView>
    
    fun findByUserOrderByViewedAtDesc(user: User): List<RecentView>
    
    @Query("SELECT rv FROM RecentView rv WHERE rv.user.id = :userId AND rv.place.id = :placeId")
    fun findByUserIdAndPlaceId(@Param("userId") userId: Long, @Param("placeId") placeId: Long): RecentView?
    
    @Modifying
    @Query("UPDATE RecentView rv SET rv.viewedAt = :viewedAt WHERE rv.user.id = :userId AND rv.place.id = :placeId")
    fun updateViewedAt(@Param("userId") userId: Long, @Param("placeId") placeId: Long, @Param("viewedAt") viewedAt: OffsetDateTime)
    
    @Modifying
    @Query("DELETE FROM RecentView rv WHERE rv.user.id = :userId AND rv.viewedAt < :cutoffTime")
    fun deleteOldViewsByUser(@Param("userId") userId: Long, @Param("cutoffTime") cutoffTime: OffsetDateTime)
}
package com.mohe.spring.repository

import com.mohe.spring.entity.Bookmark
import com.mohe.spring.entity.Place
import com.mohe.spring.entity.User
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.util.*

@Repository
interface BookmarkRepository : JpaRepository<Bookmark, Long> {
    
    fun findByUserAndPlace(user: User, place: Place): Optional<Bookmark>
    
    fun existsByUserAndPlace(user: User, place: Place): Boolean
    
    fun findByUserOrderByCreatedAtDesc(user: User, pageable: Pageable): Page<Bookmark>
    
    fun findByUserOrderByCreatedAtDesc(user: User): List<Bookmark>
    
    @Query("SELECT b.place.id FROM Bookmark b WHERE b.user.id = :userId")
    fun findBookmarkedPlaceIdsByUserId(@Param("userId") userId: Long): List<Long>
    
    fun countByUser(user: User): Long
    
    fun deleteByUserAndPlace(user: User, place: Place)
}
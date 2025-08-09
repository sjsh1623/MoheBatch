package com.mohe.spring.repository

import com.mohe.spring.entity.User
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.time.OffsetDateTime
import java.util.*

@Repository
interface UserRepository : JpaRepository<User, Long> {
    
    fun findByEmail(email: String): Optional<User>
    
    fun findByNickname(nickname: String): Optional<User>
    
    fun existsByEmail(email: String): Boolean
    
    fun existsByNickname(nickname: String): Boolean
    
    @Modifying
    @Query("UPDATE User u SET u.lastLoginAt = :loginTime WHERE u.id = :userId")
    fun updateLastLoginAt(@Param("userId") userId: Long, @Param("loginTime") loginTime: OffsetDateTime)
    
    @Modifying
    @Query("UPDATE User u SET u.isOnboardingCompleted = :completed WHERE u.id = :userId")
    fun updateOnboardingCompleted(@Param("userId") userId: Long, @Param("completed") completed: Boolean)
}
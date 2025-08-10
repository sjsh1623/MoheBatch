package com.mohe.spring.repository

import com.mohe.spring.entity.RefreshToken
import com.mohe.spring.entity.User
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.time.OffsetDateTime
import java.util.*

@Repository
interface RefreshTokenRepository : JpaRepository<RefreshToken, Long> {
    
    fun findByToken(token: String): Optional<RefreshToken>
    
    fun findByUserAndIsRevokedFalse(user: User): List<RefreshToken>
    
    @Modifying
    @Query("UPDATE RefreshToken rt SET rt.isRevoked = true WHERE rt.user.id = :userId")
    fun revokeAllByUserId(@Param("userId") userId: Long)
    
    @Modifying
    @Query("UPDATE RefreshToken rt SET rt.isRevoked = true WHERE rt.token = :token")
    fun revokeByToken(@Param("token") token: String)
    
    @Modifying
    @Query("DELETE FROM RefreshToken rt WHERE rt.expiresAt < :now")
    fun deleteExpiredTokens(@Param("now") now: OffsetDateTime)
    
    @Query("SELECT rt FROM RefreshToken rt WHERE rt.token = :token AND rt.isRevoked = false AND rt.expiresAt > :now")
    fun findValidToken(@Param("token") token: String, @Param("now") now: OffsetDateTime): Optional<RefreshToken>
}
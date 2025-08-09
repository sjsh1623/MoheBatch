package com.mohe.spring.repository

import com.mohe.spring.entity.PasswordResetToken
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.time.OffsetDateTime
import java.util.*

@Repository
interface PasswordResetTokenRepository : JpaRepository<PasswordResetToken, Long> {
    
    fun findByToken(token: String): Optional<PasswordResetToken>
    
    @Query("SELECT prt FROM PasswordResetToken prt WHERE prt.token = :token AND prt.used = false AND prt.expiresAt > :now")
    fun findValidToken(@Param("token") token: String, @Param("now") now: OffsetDateTime): Optional<PasswordResetToken>
    
    @Modifying
    @Query("UPDATE PasswordResetToken prt SET prt.used = true WHERE prt.token = :token")
    fun markTokenAsUsed(@Param("token") token: String)
    
    @Modifying
    @Query("DELETE FROM PasswordResetToken prt WHERE prt.expiresAt < :now")
    fun deleteExpiredTokens(@Param("now") now: OffsetDateTime)
}
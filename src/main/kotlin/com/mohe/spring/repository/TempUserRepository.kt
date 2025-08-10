package com.mohe.spring.repository

import com.mohe.spring.entity.TempUser
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.time.OffsetDateTime
import java.util.*

@Repository
interface TempUserRepository : JpaRepository<TempUser, String> {
    
    fun findByEmail(email: String): Optional<TempUser>
    
    @Query("SELECT tu FROM TempUser tu WHERE tu.id = :id AND tu.expiresAt > :now")
    fun findValidTempUser(@Param("id") id: String, @Param("now") now: OffsetDateTime): Optional<TempUser>
    
    @Modifying
    @Query("DELETE FROM TempUser tu WHERE tu.expiresAt < :now")
    fun deleteExpiredTempUsers(@Param("now") now: OffsetDateTime)
    
    fun existsByEmail(email: String): Boolean
}
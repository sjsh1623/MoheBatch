package com.mohe.spring.entity

import jakarta.persistence.*
import java.time.OffsetDateTime

@Entity
@Table(name = "temp_users")
data class TempUser(
    @Id
    val id: String,
    
    @Column(nullable = false)
    val email: String,
    
    @Column(name = "verification_code", nullable = false)
    val verificationCode: String,
    
    val nickname: String? = null,
    
    @Column(name = "password_hash")
    val passwordHash: String? = null,
    
    @Column(name = "terms_agreed")
    val termsAgreed: Boolean = false,
    
    @Column(name = "created_at")
    val createdAt: OffsetDateTime = OffsetDateTime.now(),
    
    @Column(name = "expires_at", nullable = false)
    val expiresAt: OffsetDateTime
)
package com.mohe.spring.entity

import jakarta.persistence.*
import java.time.OffsetDateTime

@Entity
@Table(name = "password_reset_tokens")
data class PasswordResetToken(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    val user: User,
    
    @Column(unique = true, nullable = false)
    val token: String,
    
    @Column(name = "expires_at", nullable = false)
    val expiresAt: OffsetDateTime,
    
    @Column(name = "created_at")
    val createdAt: OffsetDateTime = OffsetDateTime.now(),
    
    val used: Boolean = false
)
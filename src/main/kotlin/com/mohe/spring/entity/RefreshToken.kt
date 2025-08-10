package com.mohe.spring.entity

import jakarta.persistence.*
import java.time.OffsetDateTime

@Entity
@Table(name = "refresh_tokens")
data class RefreshToken(
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
    
    @Column(name = "is_revoked")
    val isRevoked: Boolean = false
)
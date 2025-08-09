package com.mohe.spring.entity

import jakarta.persistence.*
import java.time.OffsetDateTime

@Entity
@Table(name = "email_verifications")
data class EmailVerification(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    val user: User,
    
    @Column(nullable = false)
    val email: String,
    
    @Column(nullable = false)
    val code: String,
    
    @Column(name = "issued_at")
    val issuedAt: OffsetDateTime = OffsetDateTime.now(),
    
    @Column(name = "verified_at")
    val verifiedAt: OffsetDateTime? = null,
    
    @Column(nullable = false)
    val success: Boolean = false
)
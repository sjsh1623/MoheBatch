package com.mohe.spring.entity

import jakarta.persistence.*
import java.time.OffsetDateTime

@Entity
@Table(name = "terms_agreements")
data class TermsAgreement(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    val user: User,
    
    @Column(name = "terms_code", nullable = false)
    val termsCode: String,
    
    @Column(nullable = false)
    val agreed: Boolean = false,
    
    @Column(name = "agreed_at")
    val agreedAt: OffsetDateTime = OffsetDateTime.now()
)
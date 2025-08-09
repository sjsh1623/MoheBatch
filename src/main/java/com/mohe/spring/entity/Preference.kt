package com.mohe.spring.entity

import jakarta.persistence.*
import java.time.OffsetDateTime

@Entity
@Table(name = "preferences")
data class Preference(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    val user: User,
    
    @Column(name = "pref_key", nullable = false)
    val prefKey: String,
    
    @Column(name = "pref_value")
    val prefValue: String? = null,
    
    @Column(name = "created_at")
    val createdAt: OffsetDateTime = OffsetDateTime.now()
)
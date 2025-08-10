package com.mohe.spring.entity

import jakarta.persistence.*
import java.time.OffsetDateTime

@Entity
@Table(name = "recent_views")
data class RecentView(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    val user: User,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", nullable = false)
    val place: Place,
    
    @Column(name = "viewed_at")
    val viewedAt: OffsetDateTime = OffsetDateTime.now()
)
package com.mohe.spring.entity

import jakarta.persistence.*
import java.time.OffsetDateTime

@Entity
@Table(name = "bookmarks", uniqueConstraints = [
    UniqueConstraint(columnNames = ["user_id", "place_id"])
])
data class Bookmark(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    val user: User,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", nullable = false)
    val place: Place,
    
    @Column(name = "created_at")
    val createdAt: OffsetDateTime = OffsetDateTime.now()
)
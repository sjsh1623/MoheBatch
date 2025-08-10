package com.mohe.spring.entity

import jakarta.persistence.*
import java.time.LocalDateTime

@Entity
@Table(
    name = "place_mbti_descriptions",
    uniqueConstraints = [UniqueConstraint(columnNames = ["place_id", "mbti"])]
)
data class PlaceMbtiDescription(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,
    
    @Column(name = "place_id", nullable = false)
    val placeId: Long,
    
    @Column(nullable = false, length = 4)
    val mbti: String,
    
    @Column(nullable = false, columnDefinition = "TEXT")
    val description: String,
    
    @Column(length = 100)
    val model: String = "llama3.1:latest",
    
    @Column(name = "prompt_hash", nullable = false, length = 64)
    val promptHash: String,
    
    @Column(name = "updated_at")
    val updatedAt: LocalDateTime = LocalDateTime.now(),
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", insertable = false, updatable = false)
    val place: Place? = null
)

enum class MbtiType(val code: String) {
    INTJ("INTJ"), INTP("INTP"), ENTJ("ENTJ"), ENTP("ENTP"),
    INFJ("INFJ"), INFP("INFP"), ENFJ("ENFJ"), ENFP("ENFP"),
    ISTJ("ISTJ"), ISFJ("ISFJ"), ESTJ("ESTJ"), ESFJ("ESFJ"),
    ISTP("ISTP"), ISFP("ISFP"), ESTP("ESTP"), ESFP("ESFP");
    
    companion object {
        fun fromString(code: String): MbtiType? = values().find { it.code.equals(code, ignoreCase = true) }
        fun getAllCodes(): List<String> = values().map { it.code }
    }
}
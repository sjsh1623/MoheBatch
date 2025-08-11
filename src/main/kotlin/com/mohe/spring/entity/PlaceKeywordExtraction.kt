package com.mohe.spring.entity

import jakarta.persistence.*
import org.hibernate.annotations.Type
import java.time.LocalDateTime

@Entity
@Table(name = "place_keyword_extractions")
data class PlaceKeywordExtraction(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    
    @Column(name = "place_id", nullable = false)
    val placeId: Long,
    
    @Column(name = "raw_text", nullable = false, columnDefinition = "TEXT")
    val rawText: String, // Original place description used for extraction
    
    @Column(name = "model_name", nullable = false, length = 100)
    val modelName: String, // e.g., "ollama-openai"
    
    @Column(name = "model_version", nullable = false, length = 50)
    val modelVersion: String, // e.g., "llama3.1:latest"
    
    @Column(name = "keyword_vector", nullable = false, columnDefinition = "vector(100)")
    val keywordVector: FloatArray, // pgvector column for similarity search
    
    @Column(name = "selected_keywords", nullable = false, columnDefinition = "JSONB")
    val selectedKeywords: String, // JSONB array of {keyword, confidence_score} objects
    
    @Column(name = "extraction_method", length = 50)
    val extractionMethod: String = "ollama_llm",
    
    @Column(name = "processing_time_ms")
    val processingTimeMs: Int? = null,
    
    @Column(name = "created_at")
    val createdAt: LocalDateTime = LocalDateTime.now(),
    
    @Column(name = "updated_at")
    val updatedAt: LocalDateTime = LocalDateTime.now(),
    
    // Foreign key relationship to Place entity
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", insertable = false, updatable = false)
    val place: Place? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as PlaceKeywordExtraction

        if (id != other.id) return false
        if (placeId != other.placeId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + placeId.hashCode()
        return result
    }

    override fun toString(): String {
        return "PlaceKeywordExtraction(id=$id, placeId=$placeId, modelName='$modelName', extractionMethod='$extractionMethod')"
    }
}
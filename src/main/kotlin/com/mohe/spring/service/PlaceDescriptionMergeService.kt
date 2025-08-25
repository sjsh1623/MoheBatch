package com.mohe.spring.service

// Removed external batch DTO imports - using generic Any type instead
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.concurrent.ThreadLocalRandom

/**
 * 네이버와 구글 API 데이터를 병합하여 독창적이고 매력적인 장소 설명을 생성하는 서비스
 * 
 * 각 장소의 독특한 특성을 반영하여 일률적이지 않은 설명을 제공합니다.
 * - 음식 종류와 인기 메뉴 정보 우선 포함
 * - 분위기와 특색 강조
 * - 위치 정보는 이미 다른 필드에 존재하므로 최소화
 */
@Service
class PlaceDescriptionMergeService(
    private val objectMapper: ObjectMapper,
    @Value("\${app.description.max-length:400}") private val maxDescriptionLength: Int
) {

    private val logger = LoggerFactory.getLogger(PlaceDescriptionMergeService::class.java)

    data class MergedDescription(
        val finalDescription: String,
        val sourceInfo: Map<String, Any>,
        val style: String
    )

    /**
     * 네이버와 구글 데이터를 병합하여 독창적인 설명 생성
     * Temporary implementation using Any type until DTO integration is complete
     */
    fun mergeDescriptions(naverPlace: Any, googlePlace: Any?): MergedDescription {
        val style = "BALANCED" // Simplified for now
        val description = createSimpleDescription(naverPlace, googlePlace)

        return MergedDescription(
            finalDescription = description.take(maxDescriptionLength),
            sourceInfo = mapOf("naver" to (naverPlace != null), "google" to (googlePlace != null)),
            style = style
        )
    }

    /**
     * Simplified description creation for temporary compatibility
     */
    private fun createSimpleDescription(naverPlace: Any, googlePlace: Any?): String {
        // Temporary implementation - returns a placeholder description
        return "장소 정보가 업데이트 중입니다. 곧 자세한 정보를 제공해드리겠습니다."
    }
}
package com.mohe.spring.service

import com.example.ingestion.dto.GooglePlaceDetail
import com.example.ingestion.dto.NaverPlaceItem
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
     */
    fun mergeDescriptions(naverPlace: NaverPlaceItem, googlePlace: GooglePlaceDetail?): MergedDescription {
        val style = determineDescriptionStyle(naverPlace, googlePlace)
        val description = when (style) {
            "FOOD_FOCUSED" -> createFoodFocusedDescription(naverPlace, googlePlace)
            "ATMOSPHERE_DRIVEN" -> createAtmosphereDescription(naverPlace, googlePlace)
            "REVIEW_BASED" -> createReviewBasedDescription(naverPlace, googlePlace)
            "MINIMAL_CHIC" -> createMinimalDescription(naverPlace, googlePlace)
            else -> createBalancedDescription(naverPlace, googlePlace)
        }

        return MergedDescription(
            finalDescription = description.take(maxDescriptionLength),
            sourceInfo = buildSourceInfo(naverPlace, googlePlace),
            style = style
        )
    }

    /**
     * 장소 특성에 따라 설명 스타일 결정
     */
    private fun determineDescriptionStyle(naverPlace: NaverPlaceItem, googlePlace: GooglePlaceDetail?): String {
        val category = naverPlace.category.lowercase()
        val hasRichGoogleData = googlePlace?.reviews?.isNotEmpty() == true
        val hasMenuInfo = naverPlace.description?.contains("메뉴") == true
        
        return when {
            category.contains("카페") && hasRichGoogleData -> "ATMOSPHERE_DRIVEN"
            category.contains("음식") || category.contains("레스토랑") -> "FOOD_FOCUSED"
            hasRichGoogleData && googlePlace?.reviews?.size!! >= 3 -> "REVIEW_BASED"
            category.contains("박물관") || category.contains("갤러리") -> "MINIMAL_CHIC"
            else -> "BALANCED"
        }
    }

    /**
     * 음식점 중심의 설명 - 메뉴와 맛에 집중
     */
    private fun createFoodFocusedDescription(naverPlace: NaverPlaceItem, googlePlace: GooglePlaceDetail?): String {
        val parts = mutableListOf<String>()

        // 네이버 설명에서 음식/메뉴 관련 정보 추출
        naverPlace.description?.let { desc ->
            val foodKeywords = listOf("메뉴", "음식", "요리", "맛", "특선", "대표", "추천")
            if (foodKeywords.any { desc.contains(it) }) {
                parts.add(extractFoodInfo(desc))
            }
        }

        // 구글 리뷰에서 음식 관련 언급 추출
        googlePlace?.reviews?.take(3)?.forEach { review ->
            if (review.rating >= 4 && containsFoodMentions(review.text)) {
                val excerpt = extractFoodExcerpt(review.text)
                if (excerpt.isNotEmpty()) parts.add(excerpt)
            }
        }

        // 스타일 변형 적용
        val openings = listOf("이곳은", "여기서는", "이 식당은", "맛있는")
        val connections = listOf("특히", "또한", "무엇보다", "그리고")
        
        return if (parts.isNotEmpty()) {
            buildStyledDescription(parts, openings, connections)
        } else {
            createFallbackFoodDescription(naverPlace)
        }
    }

    /**
     * 분위기 중심의 설명 - 카페나 문화공간
     */
    private fun createAtmosphereDescription(naverPlace: NaverPlaceItem, googlePlace: GooglePlaceDetail?): String {
        val parts = mutableListOf<String>()
        
        // 분위기 키워드 매핑
        val atmosphereMap = mapOf(
            "조용한" to listOf("차분한", "평온한", "고요한"),
            "북적한" to listOf("활기찬", "생동감 있는", "역동적인"),
            "아늑한" to listOf("포근한", "편안한", "따뜻한"),
            "모던한" to listOf("세련된", "현대적인", "트렌디한")
        )

        // 구글 리뷰에서 분위기 언급 추출
        googlePlace?.reviews?.forEach { review ->
            if (review.rating >= 4) {
                val atmosphereKeywords = listOf("분위기", "인테리어", "공간", "느낌", "vibe")
                if (atmosphereKeywords.any { review.text.contains(it, ignoreCase = true) }) {
                    parts.add(extractAtmosphereInfo(review.text))
                }
            }
        }

        return if (parts.isNotEmpty()) {
            val varied = varyAtmosphereWords(parts.joinToString(" "), atmosphereMap)
            "이 곳은 $varied 매력적인 공간입니다."
        } else {
            createFallbackAtmosphereDescription(naverPlace)
        }
    }

    /**
     * 리뷰 기반 설명 - 실제 방문자 경험 중심
     */
    private fun createReviewBasedDescription(naverPlace: NaverPlaceItem, googlePlace: GooglePlaceDetail?): String {
        val goodReviews = googlePlace?.reviews?.filter { it.rating >= 4 }?.take(3) ?: emptyList()
        
        if (goodReviews.isEmpty()) {
            return createBalancedDescription(naverPlace, googlePlace)
        }

        val highlights = mutableListOf<String>()
        
        goodReviews.forEach { review ->
            // 긍정적인 키워드 추출
            val positiveKeywords = extractPositiveKeywords(review.text)
            if (positiveKeywords.isNotEmpty()) {
                highlights.addAll(positiveKeywords.take(2))
            }
        }

        val uniqueHighlights = highlights.distinct().take(4)
        
        return if (uniqueHighlights.isNotEmpty()) {
            val introductions = listOf("방문객들이 특히 좋아하는", "많은 사람들이 추천하는", "실제 이용자들이 만족하는")
            val selected = introductions[ThreadLocalRandom.current().nextInt(introductions.size)]
            "$selected 이유는 ${uniqueHighlights.joinToString(", ")} 때문입니다."
        } else {
            createBalancedDescription(naverPlace, googlePlace)
        }
    }

    /**
     * 미니멀 시크 설명 - 박물관, 갤러리 등 문화공간
     */
    private fun createMinimalDescription(naverPlace: NaverPlaceItem, googlePlace: GooglePlaceDetail?): String {
        val culturalWords = listOf("전시", "작품", "문화", "예술", "체험", "관람")
        val hascultural = naverPlace.description?.let { desc ->
            culturalWords.any { desc.contains(it) }
        } ?: false

        return if (hascultural) {
            val extracted = naverPlace.description?.let { extractCulturalInfo(it) }
            extracted?.let { "이곳에서는 $it" } ?: createDefaultMinimalDescription(naverPlace)
        } else {
            createDefaultMinimalDescription(naverPlace)
        }
    }

    /**
     * 균형잡힌 기본 설명
     */
    private fun createBalancedDescription(naverPlace: NaverPlaceItem, googlePlace: GooglePlaceDetail?): String {
        val parts = mutableListOf<String>()
        
        // 네이버 기본 정보
        naverPlace.description?.takeIf { it.isNotBlank() }?.let { parts.add(it.trim()) }
        
        // 구글 평점 정보 (높은 평점인 경우)
        googlePlace?.let { place ->
            if (place.rating != null && place.rating >= 4.0) {
                val ratingText = when {
                    place.rating >= 4.5 -> "매우 높은 평가를 받고 있는"
                    place.rating >= 4.0 -> "좋은 평가를 받는"
                    else -> ""
                }
                if (ratingText.isNotEmpty()) {
                    parts.add("${ratingText} 곳입니다.")
                }
            }
        }

        return if (parts.isNotEmpty()) {
            parts.joinToString(" ").take(maxDescriptionLength)
        } else {
            createFallbackDescription(naverPlace)
        }
    }

    // Helper methods for text processing
    private fun extractFoodInfo(description: String): String {
        // 음식 관련 정보 추출 로직
        val sentences = description.split(".", "!")
        return sentences.find { it.contains("메뉴") || it.contains("음식") || it.contains("맛") }
            ?.trim() ?: ""
    }

    private fun containsFoodMentions(text: String): Boolean {
        val foodKeywords = listOf("맛", "메뉴", "음식", "요리", "먹", "dish", "food", "taste")
        return foodKeywords.any { text.contains(it, ignoreCase = true) }
    }

    private fun extractFoodExcerpt(reviewText: String): String {
        // 리뷰에서 음식 관련 핵심 문장 추출
        val sentences = reviewText.split(".", "!")
        val foodSentence = sentences.find { containsFoodMentions(it) }
        return foodSentence?.trim()?.take(100) ?: ""
    }

    private fun extractAtmosphereInfo(reviewText: String): String {
        val sentences = reviewText.split(".", "!")
        val atmosphereKeywords = listOf("분위기", "인테리어", "공간", "느낌")
        return sentences.find { sentence -> 
            atmosphereKeywords.any { sentence.contains(it) } 
        }?.trim() ?: ""
    }

    private fun varyAtmosphereWords(text: String, atmosphereMap: Map<String, List<String>>): String {
        var result = text
        atmosphereMap.forEach { (original, alternatives) ->
            if (result.contains(original)) {
                val replacement = alternatives[ThreadLocalRandom.current().nextInt(alternatives.size)]
                result = result.replace(original, replacement)
            }
        }
        return result
    }

    private fun extractPositiveKeywords(reviewText: String): List<String> {
        val positiveWords = listOf(
            "좋", "맛있", "친절", "깨끗", "편안", "분위기", "추천", "만족", 
            "훌륭", "멋진", "아름다운", "특별", "인상적"
        )
        
        return positiveWords.filter { reviewText.contains(it) }
            .map { "${it}한 점" }
    }

    private fun extractCulturalInfo(description: String): String? {
        val culturalKeywords = listOf("전시", "작품", "문화", "예술", "체험")
        val sentences = description.split(".", "!")
        return sentences.find { sentence ->
            culturalKeywords.any { sentence.contains(it) }
        }?.trim()
    }

    private fun buildStyledDescription(parts: List<String>, openings: List<String>, connections: List<String>): String {
        if (parts.isEmpty()) return ""
        
        val opening = openings[ThreadLocalRandom.current().nextInt(openings.size)]
        val result = StringBuilder("$opening ${parts[0]}")
        
        if (parts.size > 1) {
            for (i in 1 until parts.size) {
                val connection = connections[ThreadLocalRandom.current().nextInt(connections.size)]
                result.append(". $connection ${parts[i]}")
            }
        }
        
        return result.toString()
    }

    // Fallback descriptions
    private fun createFallbackFoodDescription(naverPlace: NaverPlaceItem): String {
        val locationHint = getLocationCharacteristic(naverPlace.address)
        val categoryHint = getCategoryCharacteristic(naverPlace.category)
        return "$locationHint $categoryHint로 현지인들에게 사랑받는 곳입니다."
    }

    private fun createFallbackAtmosphereDescription(naverPlace: NaverPlaceItem): String {
        val atmosphereAdjectives = listOf("아늑한", "편안한", "세련된", "독특한", "매력적인")
        val selected = atmosphereAdjectives[ThreadLocalRandom.current().nextInt(atmosphereAdjectives.size)]
        return "${selected} 분위기의 특별한 공간입니다."
    }

    private fun createDefaultMinimalDescription(naverPlace: NaverPlaceItem): String {
        return "${naverPlace.category}로 조용히 시간을 보낼 수 있는 공간입니다."
    }

    private fun createFallbackDescription(naverPlace: NaverPlaceItem): String {
        val locationHint = getLocationCharacteristic(naverPlace.address)
        return "$locationHint 위치한 ${naverPlace.category} 공간입니다."
    }

    private fun getLocationCharacteristic(address: String): String {
        return when {
            address.contains("강남") -> "강남의 세련된"
            address.contains("홍대") || address.contains("마포") -> "홍대의 힙한"
            address.contains("성수") -> "성수의 트렌디한"
            address.contains("이태원") -> "이태원의 국제적인"
            address.contains("명동") -> "명동의 번화한"
            address.contains("종로") -> "종로의 전통과 현대가 만나는"
            else -> "서울의 특색 있는"
        }
    }

    private fun getCategoryCharacteristic(category: String): String {
        return when {
            category.contains("카페") -> "커피와 디저트를 즐길 수 있는 카페"
            category.contains("음식") || category.contains("레스토랑") -> "맛있는 식사를 제공하는 레스토랑"
            category.contains("술집") || category.contains("펭") -> "특별한 분위기의 주점"
            category.contains("베이커리") -> "갓 구운 빵의 향이 가득한 베이커리"
            else -> "독특한 매력의 공간"
        }
    }

    private fun buildSourceInfo(naverPlace: NaverPlaceItem, googlePlace: GooglePlaceDetail?): Map<String, Any> {
        return mapOf(
            "hasNaverDescription" to (naverPlace.description?.isNotBlank() == true),
            "hasGoogleReviews" to (googlePlace?.reviews?.isNotEmpty() == true),
            "googleRating" to (googlePlace?.rating ?: 0.0),
            "reviewCount" to (googlePlace?.reviews?.size ?: 0),
            "processedAt" to System.currentTimeMillis()
        )
    }
}
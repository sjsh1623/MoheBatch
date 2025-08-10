package com.example.ingestion.batch.processor

import com.example.ingestion.batch.reader.EnrichedPlace
import com.example.ingestion.dto.GoogleOpeningHours
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.LocalDateTime

data class ProcessedPlace(
    val naverPlaceId: String,
    val googlePlaceId: String?,
    val name: String,
    val description: String,
    val category: String,
    val address: String,
    val roadAddress: String?,
    val latitude: BigDecimal,
    val longitude: BigDecimal,
    val phone: String?,
    val websiteUrl: String?,
    val rating: Double?,
    val userRatingsTotal: Int?,
    val priceLevel: Int?,
    val types: List<String>,
    val openingHours: String?, // JSON string
    val imageUrl: String?,
    val sourceFlags: Map<String, Any>,
    val naverRawData: String, // JSON string
    val googleRawData: String? // JSON string
)

@Component
class PlaceEnrichmentProcessor(
    private val meterRegistry: MeterRegistry
) : ItemProcessor<EnrichedPlace, ProcessedPlace> {

    private val logger = LoggerFactory.getLogger(PlaceEnrichmentProcessor::class.java)
    private val objectMapper: ObjectMapper = jacksonObjectMapper()

    override fun process(item: EnrichedPlace): ProcessedPlace? {
        return try {
            val processedPlace = ProcessedPlace(
                naverPlaceId = generateNaverPlaceId(item),
                googlePlaceId = item.googlePlace?.placeId,
                name = item.naverPlace.cleanTitle,
                description = buildDescription(item),
                category = determineCategory(item),
                address = item.naverPlace.address,
                roadAddress = item.naverPlace.roadAddress,
                latitude = item.naverPlace.latitude,
                longitude = item.naverPlace.longitude,
                phone = determinePhone(item),
                websiteUrl = item.googlePlace?.website,
                rating = item.googlePlace?.rating,
                userRatingsTotal = item.googlePlace?.userRatingsTotal,
                priceLevel = item.googlePlace?.priceLevel,
                types = item.googlePlace?.types ?: listOf(item.naverPlace.category),
                openingHours = item.googlePlace?.openingHours?.let { serializeOpeningHours(it) },
                imageUrl = item.googlePhotoUrl,
                sourceFlags = buildSourceFlags(item),
                naverRawData = objectMapper.writeValueAsString(item.naverPlace),
                googleRawData = item.googlePlace?.let { objectMapper.writeValueAsString(it) }
            )

            logger.debug("Processed place: ${processedPlace.name} (Naver: ${processedPlace.naverPlaceId}, Google: ${processedPlace.googlePlaceId})")
            meterRegistry.counter("places_processed").increment()
            
            processedPlace
        } catch (e: Exception) {
            logger.error("Failed to process enriched place ${item.naverPlace.cleanTitle}: ${e.message}", e)
            meterRegistry.counter("places_processing_failed").increment()
            null
        }
    }

    private fun generateNaverPlaceId(item: EnrichedPlace): String {
        // Generate a consistent ID from Naver place data
        return "nv_${item.naverPlace.cleanTitle.hashCode().toString().replace("-", "n")}_${item.naverPlace.mapX}_${item.naverPlace.mapY}"
    }

    private fun buildDescription(item: EnrichedPlace): String {
        val parts = mutableListOf<String>()
        
        // Naver description
        item.naverPlace.description?.let { desc ->
            if (desc.isNotBlank()) parts.add(desc.trim())
        }
        
        // Google reviews for description
        item.googlePlace?.reviews?.take(2)?.forEach { review ->
            if (review.text.length > 50 && review.rating >= 4) {
                parts.add(review.text.take(200) + if (review.text.length > 200) "..." else "")
            }
        }
        
        // Fallback description based on category and location
        if (parts.isEmpty()) {
            val locationDesc = when {
                item.naverPlace.address.contains("강남") -> "강남구에 위치한"
                item.naverPlace.address.contains("홍대") || item.naverPlace.address.contains("마포") -> "홍대 지역의"
                item.naverPlace.address.contains("성수") -> "성수동의 트렌디한"
                item.naverPlace.address.contains("이태원") -> "이태원의 국제적인"
                item.naverPlace.address.contains("명동") || item.naverPlace.address.contains("중구") -> "명동 중심가의"
                item.naverPlace.address.contains("종로") -> "종로구의 전통적인"
                else -> "서울 지역의"
            }
            
            val categoryDesc = when {
                item.naverPlace.category.contains("카페") -> "아늑한 카페"
                item.naverPlace.category.contains("음식") || item.naverPlace.category.contains("레스토랑") -> "맛있는 음식점"
                item.naverPlace.category.contains("펍") || item.naverPlace.category.contains("바") -> "분위기 좋은 주점"
                item.naverPlace.category.contains("베이커리") -> "신선한 베이커리"
                item.naverPlace.category.contains("문화") || item.naverPlace.category.contains("박물관") -> "문화 공간"
                else -> "특별한 장소"
            }
            
            parts.add("$locationDesc $categoryDesc 입니다.")
        }
        
        return parts.joinToString(" ").take(500)
    }

    private fun determineCategory(item: EnrichedPlace): String {
        // Prioritize Google types for better categorization
        item.googlePlace?.types?.let { types ->
            return when {
                types.any { it in listOf("cafe", "bakery") } -> "카페"
                types.any { it in listOf("restaurant", "food", "meal_takeaway") } -> "음식점"
                types.any { it in listOf("bar", "night_club") } -> "바"
                types.any { it in listOf("park", "tourist_attraction") } -> "관광지"
                types.any { it in listOf("museum", "art_gallery") } -> "문화시설"
                types.any { it in listOf("shopping_mall", "store") } -> "쇼핑"
                types.any { it in listOf("gym", "spa") } -> "웰니스"
                else -> item.naverPlace.category
            }
        }
        
        return item.naverPlace.category
    }

    private fun determinePhone(item: EnrichedPlace): String? {
        // Prefer Google's formatted phone number
        return item.googlePlace?.formattedPhoneNumber?.takeIf { it.isNotBlank() }
            ?: item.naverPlace.telephone?.takeIf { it.isNotBlank() }
    }

    private fun serializeOpeningHours(openingHours: GoogleOpeningHours): String {
        return try {
            objectMapper.writeValueAsString(openingHours)
        } catch (e: Exception) {
            logger.warn("Failed to serialize opening hours: ${e.message}")
            "{}"
        }
    }

    private fun buildSourceFlags(item: EnrichedPlace): Map<String, Any> {
        return mapOf(
            "hasNaverData" to true,
            "hasGoogleData" to (item.googlePlace != null),
            "hasGooglePhoto" to (item.googlePhotoUrl != null),
            "hasGoogleReviews" to (item.googlePlace?.reviews?.isNotEmpty() == true),
            "hasOpeningHours" to (item.googlePlace?.openingHours != null),
            "hasRating" to (item.googlePlace?.rating != null),
            "searchQuery" to item.searchContext.query,
            "searchArea" to "${item.searchContext.coordinate.lat},${item.searchContext.coordinate.lng}",
            "processedAt" to LocalDateTime.now().toString()
        )
    }
}
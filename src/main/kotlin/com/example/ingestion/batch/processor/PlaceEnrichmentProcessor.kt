package com.example.ingestion.batch.processor

import com.example.ingestion.batch.reader.EnrichedPlace
import com.example.ingestion.dto.GoogleOpeningHours
import com.example.ingestion.dto.ProcessedPlace
// Removed dependency on MoheSpring service - using local implementation
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.LocalDateTime

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
                description = buildEnhancedDescription(item),
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
                gallery = if (item.googlePhotoUrl != null) listOf(item.googlePhotoUrl) else emptyList(),
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

    /**
     * 향상된 설명 생성 - Local implementation instead of external service dependency
     */
    private fun buildEnhancedDescription(item: EnrichedPlace): String {
        return try {
            val description = buildFallbackDescription(item)
            logger.debug("Generated description for ${item.naverPlace.cleanTitle}")
            meterRegistry.counter("description_generation_success").increment()
            description
        } catch (e: Exception) {
            logger.warn("Failed to generate description for ${item.naverPlace.cleanTitle}: ${e.message}")
            meterRegistry.counter("description_generation_failed").increment()
            "${item.naverPlace.category} 카테고리의 장소입니다."
        }
    }

    /**
     * 대체 설명 생성 (enhanced description 생성 실패 시 사용)
     */
    private fun buildFallbackDescription(item: EnrichedPlace): String {
        val parts = mutableListOf<String>()
        
        // Naver description
        item.naverPlace.description?.let { desc ->
            if (desc.isNotBlank()) parts.add(desc.trim())
        }
        
        // Google reviews for description
        item.googlePlace?.reviews?.take(1)?.forEach { review ->
            if (review.text.length > 50 && review.rating >= 4) {
                parts.add(review.text.take(150) + if (review.text.length > 150) "..." else "")
            }
        }
        
        // Simple fallback based on category
        if (parts.isEmpty()) {
            val categoryDesc = when {
                item.naverPlace.category.contains("카페") -> "커피와 디저트를 즐길 수 있는 카페입니다."
                item.naverPlace.category.contains("음식") -> "맛있는 음식을 제공하는 레스토랑입니다."
                else -> "${item.naverPlace.category} 카테고리의 장소입니다."
            }
            parts.add(categoryDesc)
        }
        
        return parts.joinToString(" ").take(400)
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
            "processedAt" to LocalDateTime.now().toString(),
            "enhancedDescriptionEnabled" to true,
            "processingVersion" to "v2.0"
        )
    }
}
package com.example.ingestion.batch.processor

import com.example.ingestion.batch.reader.EnrichedPlace
import com.example.ingestion.dto.*
// import com.example.ingestion.service.OllamaService - not used in this implementation
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.util.retry.Retry
import java.math.BigDecimal
import java.time.Duration
import java.util.*

@Component
class RegionalPlaceEnrichmentProcessor(
    private val webClient: WebClient,
    private val meterRegistry: MeterRegistry,
    private val objectMapper: ObjectMapper,
    @Value("\${OLLAMA_HOST:http://localhost:11434}") private val ollamaHost: String,
    @Value("\${OLLAMA_TEXT_MODEL:gpt-oss:20b}") private val textModel: String,
    @Value("\${OLLAMA_EMBEDDING_MODEL:mxbai-embed-large:latest}") private val embeddingModel: String,
    @Value("\${OLLAMA_TIMEOUT:120}") private val ollamaTimeout: Int,
    @Value("\${app.external.google.api-key}") private val googleApiKey: String
) : ItemProcessor<EnrichedPlace, ProcessedPlace> {

    private val logger = LoggerFactory.getLogger(RegionalPlaceEnrichmentProcessor::class.java)

    override fun process(item: EnrichedPlace): ProcessedPlace? {
        return try {
            logger.debug("Processing place: ${item.naverPlace.cleanTitle}")
            
            // Basic validation
            if (item.naverPlace.cleanTitle.isBlank()) {
                logger.warn("Skipping place with empty title")
                return null
            }

            // Generate AI-powered description using Ollama
            val enhancedDescription = generateEnhancedDescription(item)
            
            // Generate keyword vector embeddings
            val keywordVector = generateKeywordVector(item, enhancedDescription)
            
            // Build comprehensive place data
            val processedPlace = ProcessedPlace(
                naverPlaceId = generateNaverPlaceId(item.naverPlace),
                googlePlaceId = item.googlePlace?.placeId,
                name = item.naverPlace.cleanTitle,
                description = enhancedDescription,
                category = determineCategory(item),
                address = item.naverPlace.address,
                roadAddress = item.naverPlace.roadAddress,
                latitude = BigDecimal(item.naverPlace.latitude.toString()),
                longitude = BigDecimal(item.naverPlace.longitude.toString()),
                phone = item.googlePlace?.formattedPhoneNumber ?: extractPhoneFromNaver(item.naverPlace),
                websiteUrl = item.googlePlace?.website,
                rating = item.googlePlace?.rating ?: parseNaverRating(item.naverPlace),
                userRatingsTotal = item.googlePlace?.userRatingsTotal,
                priceLevel = item.googlePlace?.priceLevel,
                types = combineTypes(item),
                openingHours = item.googlePlace?.openingHours?.let { objectMapper.writeValueAsString(it) },
                imageUrl = item.googlePhotoUrl ?: extractImageFromNaver(item.naverPlace),
                images = collectMultipleImages(item),
                sourceFlags = mapOf(
                    "hasNaverData" to true,
                    "hasGoogleData" to (item.googlePlace != null),
                    "hasAiDescription" to (enhancedDescription != item.naverPlace.description),
                    "hasKeywordVector" to (keywordVector.isNotEmpty()),
                    "processingTimestamp" to System.currentTimeMillis()
                ),
                naverRawData = objectMapper.writeValueAsString(item.naverPlace),
                googleRawData = item.googlePlace?.let { objectMapper.writeValueAsString(it) },
                keywordVector = keywordVector
            )
            
            meterRegistry.counter("regional_places_processed_success").increment()
            logger.debug("Successfully processed place: ${item.naverPlace.cleanTitle}")
            
            processedPlace
        } catch (e: Exception) {
            logger.error("Failed to process place ${item.naverPlace.cleanTitle}: ${e.message}", e)
            meterRegistry.counter("regional_places_processed_error").increment()
            null
        }
    }

    @Retryable(
        value = [WebClientResponseException::class],
        maxAttempts = 3,
        backoff = Backoff(delay = 2000, multiplier = 2.0, maxDelay = 30000)
    )
    private fun generateEnhancedDescription(item: EnrichedPlace): String {
        return try {
            val contextInfo = buildContextForDescription(item)
            val prompt = buildDescriptionPrompt(item, contextInfo)
            
            logger.debug("Generating AI description for: ${item.naverPlace.cleanTitle}")
            
            val response = webClient.post()
                .uri("$ollamaHost/api/generate")
                .bodyValue(mapOf(
                    "model" to textModel,
                    "prompt" to prompt,
                    "stream" to false,
                    "options" to mapOf(
                        "temperature" to 0.7,
                        "max_tokens" to 300,
                        "top_p" to 0.9
                    )
                ))
                .retrieve()
                .bodyToMono(OllamaTextResponse::class.java)
                .retryWhen(
                    Retry.backoff(2, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofSeconds(10))
                        .filter { it is WebClientResponseException }
                )
                .block(Duration.ofSeconds(ollamaTimeout.toLong()))

            val aiDescription = response?.response?.trim() ?: ""
            
            if (aiDescription.isNotBlank() && aiDescription.length > 20) {
                meterRegistry.counter("ollama_description_generated").increment()
                aiDescription
            } else {
                logger.warn("AI description generation failed or returned insufficient content for ${item.naverPlace.cleanTitle}")
                fallbackDescription(item)
            }
        } catch (e: Exception) {
            logger.warn("Failed to generate AI description for ${item.naverPlace.cleanTitle}: ${e.message}")
            meterRegistry.counter("ollama_description_failed").increment()
            fallbackDescription(item)
        }
    }

    @Retryable(
        value = [WebClientResponseException::class],
        maxAttempts = 3,
        backoff = Backoff(delay = 2000, multiplier = 2.0, maxDelay = 30000)
    )
    private fun generateKeywordVector(item: EnrichedPlace, description: String): List<Double> {
        return try {
            val textForEmbedding = buildTextForEmbedding(item, description)
            
            logger.debug("Generating keyword vector for: ${item.naverPlace.cleanTitle}")
            
            val response = webClient.post()
                .uri("$ollamaHost/api/embeddings")
                .bodyValue(mapOf(
                    "model" to embeddingModel,
                    "prompt" to textForEmbedding
                ))
                .retrieve()
                .bodyToMono(OllamaEmbeddingResponse::class.java)
                .retryWhen(
                    Retry.backoff(2, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofSeconds(10))
                        .filter { it is WebClientResponseException }
                )
                .block(Duration.ofSeconds(ollamaTimeout.toLong()))

            val embedding = response?.embedding ?: emptyList()
            
            if (embedding.isNotEmpty()) {
                meterRegistry.counter("ollama_embeddings_generated").increment()
                meterRegistry.gauge("ollama_embedding_dimensions", embedding.size)
                embedding
            } else {
                logger.warn("Embedding generation failed for ${item.naverPlace.cleanTitle}")
                meterRegistry.counter("ollama_embeddings_failed").increment()
                emptyList()
            }
        } catch (e: Exception) {
            logger.warn("Failed to generate embedding for ${item.naverPlace.cleanTitle}: ${e.message}")
            meterRegistry.counter("ollama_embeddings_failed").increment()
            emptyList()
        }
    }

    private fun buildDescriptionPrompt(item: EnrichedPlace, contextInfo: String): String {
        val placeName = item.naverPlace.cleanTitle
        val category = item.naverPlace.category
        val originalDesc = item.naverPlace.description
        
        return """
장소의 분위기와 경험에 대한 생생한 설명을 작성해 주세요.

장소 정보:
- 이름: $placeName
- 카테고리: $category  
- 기존 설명: $originalDesc
- 추가 정보: $contextInfo

요구사항:
1. 200-300자 내외의 한국어 설명을 작성해 주세요
2. 장소의 분위기, 음향, 조명, 인테리어 스타일을 생생하게 묘사해 주세요
3. 사람들의 활동 양상과 붐비는 정도를 설명해 주세요 (조용한지, 활기찬지, 대화하는 소리, 음악 등)
4. 방문객이 느낄 수 있는 감정과 경험을 중심으로 작성해 주세요
5. 주소나 구체적인 위치 정보는 절대 포함하지 마세요
6. 시간대별 분위기 차이가 있다면 언급해 주세요
7. 공간의 크기감과 좌석 배치, 프라이빗함 정도를 설명해 주세요

분위기 중심 설명:
        """.trimIndent()
    }

    private fun buildTextForEmbedding(item: EnrichedPlace, description: String): String {
        val components = mutableListOf<String>()
        
        components.add(item.naverPlace.cleanTitle)
        components.add(item.naverPlace.category)
        components.add(description)
        
        // Add Google Place types if available
        item.googlePlace?.types?.let { types ->
            components.addAll(types)
        }
        
        // Add location information
        components.add(item.naverPlace.address)
        item.naverPlace.roadAddress?.let { components.add(it) }
        
        return components.filter { it.isNotBlank() }.joinToString(" ")
    }

    private fun buildContextForDescription(item: EnrichedPlace): String {
        val context = mutableListOf<String>()
        
        // Add rating information
        item.googlePlace?.rating?.let { rating ->
            context.add("평점 ${rating}/5.0")
            item.googlePlace.userRatingsTotal?.let { total ->
                context.add("(${total}개 리뷰)")
            }
        }
        
        // Add price level
        item.googlePlace?.priceLevel?.let { priceLevel ->
            val priceDescription = when (priceLevel) {
                0 -> "무료"
                1 -> "저렴한 가격대"
                2 -> "적당한 가격대"  
                3 -> "비싼 가격대"
                4 -> "매우 비싼 가격대"
                else -> "가격 정보 있음"
            }
            context.add(priceDescription)
        }
        
        // Add opening hours info
        item.googlePlace?.openingHours?.let {
            context.add("영업시간 정보 제공")
        }
        
        // Add website info
        item.googlePlace?.website?.let {
            context.add("공식 웹사이트 있음")
        }
        
        return context.joinToString(", ")
    }

    private fun fallbackDescription(item: EnrichedPlace): String {
        val placeName = item.naverPlace.cleanTitle
        val category = item.naverPlace.category
        val address = item.naverPlace.address
        
        return when {
            item.naverPlace.description?.isNotBlank() == true -> item.naverPlace.description!!
            else -> "${placeName}은(는) ${address}에 위치한 ${category} 장소입니다."
        }
    }

    private fun generateNaverPlaceId(naverPlace: NaverPlaceItem): String {
        // Generate a unique ID based on Naver place data
        val identifier = "${naverPlace.cleanTitle}-${naverPlace.address}-${naverPlace.latitude}-${naverPlace.longitude}"
        return Base64.getEncoder().encodeToString(identifier.toByteArray()).take(20)
    }

    private fun determineCategory(item: EnrichedPlace): String {
        return item.naverPlace.category.ifBlank { 
            item.googlePlace?.types?.firstOrNull() ?: "장소" 
        }
    }

    private fun extractPhoneFromNaver(naverPlace: NaverPlaceItem): String? {
        // Naver API sometimes includes phone in description or other fields
        return null // Naver Local API typically doesn't provide phone numbers directly
    }

    private fun extractImageFromNaver(naverPlace: NaverPlaceItem): String? {
        // Extract image URL from Naver place data
        return naverPlace.link?.takeIf { it.isNotBlank() }
    }

    private fun collectMultipleImages(item: EnrichedPlace): List<String> {
        val imageUrls = mutableListOf<String>()
        
        try {
            // Collect multiple images from Google Places API
            item.googlePlace?.photos?.let { photos ->
                // Take 3-10 photos (user requested minimum 3, maximum 10)
                val photoCount = minOf(10, maxOf(3, photos.size))
                
                logger.debug("Collecting $photoCount images from ${photos.size} available photos for ${item.naverPlace.cleanTitle}")
                
                photos.take(photoCount).forEach { photo ->
                    try {
                        val photoUrl = "https://maps.googleapis.com/maps/api/place/photo?maxwidth=800&photo_reference=${photo.photoReference}&key=${googleApiKey}"
                        imageUrls.add(photoUrl)
                        logger.debug("Added photo URL for ${item.naverPlace.cleanTitle}: $photoUrl")
                    } catch (e: Exception) {
                        logger.warn("Failed to build photo URL for ${item.naverPlace.cleanTitle}: ${e.message}")
                    }
                }
            }
            
            // If we don't have enough Google Photos, try to add Naver image
            if (imageUrls.size < 3) {
                extractImageFromNaver(item.naverPlace)?.let { naverImage ->
                    imageUrls.add(naverImage)
                    logger.debug("Added Naver image to reach minimum count: $naverImage")
                }
            }
            
            logger.debug("Collected ${imageUrls.size} images for ${item.naverPlace.cleanTitle}")
            
        } catch (e: Exception) {
            logger.warn("Failed to collect multiple images for ${item.naverPlace.cleanTitle}: ${e.message}")
        }
        
        return imageUrls.distinct() // Remove duplicates
    }

    private fun parseNaverRating(naverPlace: NaverPlaceItem): Double? {
        // Try to extract rating from Naver data if available
        return null // Naver Local API typically doesn't provide ratings
    }

    private fun combineTypes(item: EnrichedPlace): List<String> {
        val types = mutableSetOf<String>()
        
        // Add Naver category
        if (item.naverPlace.category.isNotBlank()) {
            types.add(item.naverPlace.category)
        }
        
        // Add Google Place types
        item.googlePlace?.types?.let { googleTypes ->
            types.addAll(googleTypes)
        }
        
        return types.toList()
    }
}

// DTOs for Ollama API responses
data class OllamaTextResponse(
    val response: String,
    val done: Boolean,
    val model: String? = null,
    val created_at: String? = null
)

data class OllamaEmbeddingResponse(
    val embedding: List<Double>,
    val model: String? = null
)
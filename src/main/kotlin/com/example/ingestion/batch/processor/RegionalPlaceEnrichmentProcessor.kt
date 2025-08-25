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
            
            // Duplicate checking - skip if place already exists with same location
            val uniqueIdentifier = "${item.naverPlace.cleanTitle}-${item.naverPlace.latitude}-${item.naverPlace.longitude}"
            if (isRecentlyProcessed(uniqueIdentifier)) {
                logger.debug("Skipping duplicate place: ${item.naverPlace.cleanTitle}")
                meterRegistry.counter("places_skipped_duplicate").increment()
                return null
            }
            
            // Mark as processed for duplicate prevention
            markAsProcessed(uniqueIdentifier)

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
이 장소에 대한 매력적이고 생생한 설명을 작성해 주세요.

장소 정보:
- 이름: $placeName
- 카테고리: $category  
- 기존 설명: $originalDesc
- 추가 정보: $contextInfo

작성 가이드:
1. 200-300자로 충분히 설명적이고 정보가 풍부하게 작성
2. 캐주얼한 존댓말로 실제 체험담처럼 작성 (친근하지만 예의 있게)
3. 분위기, 특징, 매력 포인트를 구체적으로 묘사
4. 어떤 사람들이 좋아할지, 무엇이 특별한지 설명
5. 절대 금지: 주소, 위치, 전화번호, 운영시간, 구체적 장소명 언급 금지
6. 격식체("이곳은", "해당 장소는") 사용 금지
7. 캐주얼 존댓말 사용 ("분위기 좋아요", "정말 맘에 들어요", "추천할 만해요", "~더라구요")

예시 스타일: "여기 정말 분위기 좋더라구요. 인테리어도 감각적이고 사람들이 편안하게 있을 수 있는 공간이에요. 특히 조명이 따뜻해서 친구들이랑 오기 딱 좋고, 음식도 맛있어서 재방문 의사 100%예요."

생생한 체험 후기:
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
        
        return when {
            item.naverPlace.description?.isNotBlank() == true -> {
                // Clean existing description - remove addresses if present
                val cleanDesc = item.naverPlace.description!!
                    .replace(Regex("서울특별시.*?[0-9-]+[가-힣]*[0-9]*층?호?"), "") // Remove Seoul addresses
                    .replace(Regex("경기도.*?[0-9-]+[가-힣]*[0-9]*층?호?"), "") // Remove Gyeonggi addresses  
                    .replace(Regex("인천광역시.*?[0-9-]+[가-힣]*[0-9]*층?호?"), "") // Remove Incheon addresses
                    .replace(Regex("[0-9]{2,3}-[0-9]{3,4}-[0-9]{4}"), "") // Remove phone numbers
                    .replace(Regex("전화.*?[0-9-]+"), "") // Remove phone info
                    .replace(Regex("주소.*"), "") // Remove address info
                    .trim()
                
                if (cleanDesc.length > 10) cleanDesc else generateNaturalFallback(placeName, category)
            }
            else -> generateNaturalFallback(placeName, category)
        }
    }
    
    private fun generateNaturalFallback(placeName: String, category: String): String {
        return when {
            category.contains("카페") || category.contains("커피") -> 
                "$placeName 여기 분위기 정말 좋아요. 커피도 맛있고 앉아서 이야기하기 딱 좋은 공간이에요. 많은 분들이 찾는 이유가 있더라구요."
            category.contains("음식점") || category.contains("레스토랑") || category.contains("한식") -> 
                "$placeName 맛집으로 유명한 곳이에요. 음식도 맛있고 분위기도 좋아서 사람들이 자주 가는 곳이더라구요. 추천할 만해요."
            category.contains("베이커리") || category.contains("빵") -> 
                "$placeName 빵이 정말 맛있는 곳이에요. 갓 구운 빵 냄새가 너무 좋고 종류도 다양해서 선택의 재미가 있어요."
            category.contains("주유소") -> 
                "$placeName 접근성 좋은 주유소예요. 직원분들도 친절하고 부대시설도 깔끔하게 관리되는 곳이더라구요."
            category.contains("병원") || category.contains("의료") -> 
                "$placeName 시설 깔끔하고 직원분들 친절한 곳이에요. 대기시간도 적당하고 진료 받기 좋은 환경이더라구요."
            category.contains("은행") || category.contains("금융") -> 
                "$placeName 업무 보기 편한 곳이에요. 직원분들 친절하고 시설도 깔끔해서 금융 업무 처리하기 좋더라구요."
            category.contains("헬스") || category.contains("체육") -> 
                "$placeName 운동하기 좋은 환경이에요. 시설도 잘 되어 있고 분위기도 좋아서 꾸준히 다니기 좋은 곳이더라구요."
            category.contains("쇼핑") || category.contains("마트") -> 
                "$placeName 물건 구하기 편한 곳이에요. 종류도 다양하고 가격도 합리적이어서 자주 이용하게 되는 곳이더라구요."
            else -> 
                "$placeName 괜찮은 곳이에요. 사람들이 많이 찾는 이유가 있더라구요. 한 번 가보시면 왜 인기인지 알 수 있을 거예요."
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
    
    // Simple in-memory duplicate tracking (resets per job execution)
    companion object {
        private val processedPlaces = mutableSetOf<String>()
    }
    
    private fun isRecentlyProcessed(identifier: String): Boolean {
        return processedPlaces.contains(identifier)
    }
    
    private fun markAsProcessed(identifier: String) {
        processedPlaces.add(identifier)
        // Keep only recent 10000 entries to prevent memory issues
        if (processedPlaces.size > 10000) {
            val toRemove = processedPlaces.take(processedPlaces.size - 8000)
            processedPlaces.removeAll(toRemove.toSet())
        }
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
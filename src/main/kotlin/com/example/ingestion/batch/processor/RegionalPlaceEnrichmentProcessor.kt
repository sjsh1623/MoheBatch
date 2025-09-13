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
                gallery = emptyList(),
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
                .block(Duration.ofSeconds(120)) // Increased timeout to ensure complete descriptions

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
                .block(Duration.ofSeconds(120)) // Increased timeout to ensure complete descriptions

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
        val reviewContext = extractReviewInsights(item)
        val uniqueFeatures = identifyUniqueFeatures(item)
        
        return """
이 장소를 직접 방문해서 느낀 생생한 체험담을 친근한 존댓말로 작성해 주세요.

장소 정보:
- 이름: $placeName
- 카테고리: $category  
- 기존 설명: $originalDesc
- 평점 및 리뷰 정보: $reviewContext
- 고유 특징: $uniqueFeatures
- 추가 정보: $contextInfo

작성 가이드 (공감과 친근함이 핵심):
1. 500-800자 분량으로 이 장소만의 특별한 매력을 진심이 담긴 존댓말로 작성
2. 반드시 3문장 이상의 완전한 문장으로 구성하여 자연스럽게 읽히도록 작성
3. "여기 정말 좋아요", "추천드려요", "기분이 좋아져요" 같은 친근하고 공감 가는 존댓말 사용
4. 실제 방문객의 솔직한 후기처럼 개인적인 감정과 경험을 생생하게 표현
5. 리뷰가 있다면 "다른 분들도 이렇게 말씀하시더라고요" 식으로 자연스럽게 반영
6. 방문 순간의 첫인상, 머무는 동안의 기분, 떠날 때의 아쉬움까지 감정의 변화를 담아서 작성
7. "이런 분들께 정말 추천하고 싶어요", "혼자 가기에도 좋고 친구들과 함께해도 좋아요" 같이 공감대 형성
8. 계절감, 시간대별 분위기, 날씨에 따른 느낌의 차이까지 디테일하게 묘사
9. 절대 금지: 주소, 위치 정보, 전화번호, 운영시간, 가격 정보 언급 금지
10. 똑같은 표현 반복 금지 - 매 장소마다 완전히 다른 개성과 감성으로 작성
11. "진짜", "정말", "완전" 같은 자연스러운 감탄사를 적절히 사용하여 생동감 있게 표현

마치 친한 친구에게 추천하는 듯한 따뜻하고 정감 있는 후기:
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
    
    private fun extractReviewInsights(item: EnrichedPlace): String {
        val insights = mutableListOf<String>()
        
        // Google Place 리뷰 분석
        item.googlePlace?.let { googlePlace ->
            googlePlace.rating?.let { rating ->
                when {
                    rating >= 4.5 -> insights.add("매우 높은 평점 ${rating}/5.0 (방문객들이 극찬)")
                    rating >= 4.0 -> insights.add("높은 평점 ${rating}/5.0 (만족도 우수)")
                    rating >= 3.5 -> insights.add("괜찮은 평점 ${rating}/5.0")
                    else -> insights.add("평점 ${rating}/5.0")
                }
            }
            
            googlePlace.userRatingsTotal?.let { total ->
                when {
                    total > 1000 -> insights.add("${total}명이 평가한 인기 장소")
                    total > 100 -> insights.add("${total}명의 방문객 평가")
                    total > 10 -> insights.add("${total}명이 리뷰 작성")
                    else -> insights.add("${total}명 평가")
                }
            }
            
            // Google 리뷰에서 자주 언급되는 키워드 패턴 분석
            googlePlace.reviews?.let { reviews ->
                val reviewText = reviews.joinToString(" ") { it.text ?: "" }
                val positiveKeywords = listOf("맛있", "친절", "분위기", "깔끔", "넓", "편안", "좋", "추천")
                val mentions = positiveKeywords.filter { keyword -> 
                    reviewText.contains(keyword) 
                }.take(3)
                
                if (mentions.isNotEmpty()) {
                    insights.add("리뷰에서 ${mentions.joinToString(", ")} 언급 많음")
                }
            }
        }
        
        return if (insights.isEmpty()) "리뷰 정보 없음" else insights.joinToString(", ")
    }
    
    private fun identifyUniqueFeatures(item: EnrichedPlace): String {
        val features = mutableListOf<String>()
        
        // 장소명에서 특징 추출
        val placeName = item.naverPlace.cleanTitle.lowercase()
        when {
            placeName.contains("루프톱") || placeName.contains("rooftop") -> features.add("루프톱 뷰")
            placeName.contains("24시간") || placeName.contains("24") -> features.add("24시간 운영")
            placeName.contains("드라이브") -> features.add("드라이브스루 가능")
            placeName.contains("애견") || placeName.contains("펫") -> features.add("반려동물 동반 가능")
            placeName.contains("노키즈") -> features.add("성인 전용 공간")
            placeName.contains("스터디") -> features.add("스터디하기 좋음")
            placeName.contains("데이트") -> features.add("데이트 명소")
            placeName.contains("혼밥") || placeName.contains("혼술") -> features.add("혼자 가기 좋음")
        }
        
        // Google Place 타입에서 특징 추출
        item.googlePlace?.types?.let { types ->
            types.forEach { type ->
                when (type) {
                    "night_club" -> features.add("밤문화 즐기기")
                    "bar" -> features.add("술과 분위기")
                    "cafe" -> if (!features.contains("커피 전문")) features.add("커피와 디저트")
                    "bakery" -> features.add("갓 구운 빵과 페스트리")
                    "meal_takeaway" -> features.add("포장 주문 가능")
                    "meal_delivery" -> features.add("배달 서비스")
                    "parking" -> features.add("주차 편리")
                }
            }
        }
        
        // 카테고리별 세부 특징
        val category = item.naverPlace.category.lowercase()
        when {
            category.contains("이탈리안") -> features.add("정통 이탈리아 요리")
            category.contains("일식") || category.contains("스시") -> features.add("일본 전통 요리")
            category.contains("중식") -> features.add("중화 요리 전문")
            category.contains("한식") -> features.add("전통 한국 음식")
            category.contains("디저트") -> features.add("달콤한 디저트 맛집")
            category.contains("브런치") -> features.add("여유로운 브런치")
            category.contains("바") -> features.add("칵테일과 안주")
            category.contains("헬스") -> features.add("운동과 건강관리")
            category.contains("스파") -> features.add("힐링과 휴식")
        }
        
        // 가격대 특징 (Google Place 기준)
        item.googlePlace?.priceLevel?.let { priceLevel ->
            when (priceLevel) {
                0 -> features.add("무료 이용")
                1 -> features.add("저렴한 가격대")
                2 -> features.add("합리적인 가격")
                3 -> features.add("프리미엄 서비스")
                4 -> features.add("럭셔리 경험")
                else -> features.add("가격 정보 있음")
            }
        }
        
        return if (features.isEmpty()) "일반적인 ${item.naverPlace.category}" else features.distinct().take(4).joinToString(", ")
    }

    private fun fallbackDescription(item: EnrichedPlace): String {
        val placeName = item.naverPlace.cleanTitle
        val category = item.naverPlace.category
        val uniqueFeatures = identifyUniqueFeatures(item)
        val reviewInsights = extractReviewInsights(item)
        
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
                
                if (cleanDesc.length > 10) {
                    // Enhance with unique features
                    enhanceDescriptionWithFeatures(cleanDesc, uniqueFeatures, reviewInsights)
                } else {
                    generatePersonalizedFallback(placeName, category, uniqueFeatures, reviewInsights)
                }
            }
            else -> generatePersonalizedFallback(placeName, category, uniqueFeatures, reviewInsights)
        }
    }
    
    private fun enhanceDescriptionWithFeatures(baseDesc: String, features: String, reviews: String): String {
        var enhanced = baseDesc
        
        // 리뷰 정보가 있으면 추가
        if (!reviews.contains("리뷰 정보 없음")) {
            enhanced += " $reviews"
        }
        
        // 고유 특징이 있으면 추가
        if (!features.contains("일반적인")) {
            enhanced += " 특히 $features 으로 유명한 곳이에요"
        }
        
        return enhanced.take(300) // 길이 제한
    }
    
    private fun generatePersonalizedFallback(placeName: String, category: String, features: String, reviews: String): String {
        // 리뷰 기반 설명 생성
        val baseDescription = when {
            category.contains("카페") || category.contains("커피") -> generateCafeDescription(placeName, features, reviews)
            category.contains("음식점") || category.contains("레스토랑") || category.contains("한식") -> generateRestaurantDescription(placeName, features, reviews)
            category.contains("베이커리") || category.contains("빵") -> generateBakeryDescription(placeName, features, reviews)
            category.contains("주유소") -> generateGasStationDescription(placeName, features, reviews)
            category.contains("병원") || category.contains("의료") || category.contains("약국") -> generateMedicalDescription(placeName, features, reviews)
            category.contains("은행") || category.contains("금융") -> generateBankDescription(placeName, features, reviews)
            category.contains("헬스") || category.contains("체육") || category.contains("피트니스") || category.contains("필라테스") || category.contains("짐") -> generateFitnessDescription(placeName, features, reviews)
            category.contains("쇼핑") || category.contains("마트") -> generateShoppingDescription(placeName, features, reviews)
            category.contains("바") || category.contains("펍") -> generateBarDescription(placeName, features, reviews)
            category.contains("호텔") || category.contains("펜션") -> generateAccommodationDescription(placeName, features, reviews)
            else -> generateGenericDescription(placeName, category, features, reviews)
        }
        
        // 리뷰 정보 추가
        return if (!reviews.contains("리뷰 정보 없음")) {
            "$baseDescription $reviews 더라구요."
        } else {
            baseDescription
        }
    }
    
    private fun generateCafeDescription(placeName: String, features: String, reviews: String): String {
        val sentence1 = listOf(
            "$placeName 커피 향이 진짜 좋고 인테리어도 감각적이에요",
            "$placeName 여기서 마시는 커피는 정말 특별해요",
            "$placeName 분위기가 아늑하고 편안한 느낌이 들어요",
            "$placeName 창가 자리에 앉아서 여유롭게 시간 보내기 딱 좋아요"
        ).random()
        
        val sentence2 = listOf(
            "디저트도 맛있고 사진 찍기도 예쁜 곳이에요",
            "조용히 작업하거나 친구랑 수다떨기 좋은 공간이에요",
            "원두 선택도 다양하고 바리스타분이 정성껏 내려주시네요",
            "음악도 좋고 전체적으로 세련된 분위기가 인상적이에요"
        ).random()
        
        val sentence3 = if (!features.contains("일반적인")) {
            "$features 때문에 더욱 매력적인 곳이라고 생각해요"
        } else {
            "재방문 의사가 있을 정도로 만족스러운 카페예요"
        }
        
        return "$sentence1. $sentence2. $sentence3."
    }
    
    private fun generateRestaurantDescription(placeName: String, features: String, reviews: String): String {
        val sentence1 = listOf(
            "$placeName 음식이 정말 맛있고 재료도 신선해서 만족스러워요",
            "$placeName 여기는 진짜 숨은 맛집이더라구요",
            "$placeName 가격 대비 퀄리티가 엄청 좋은 곳이에요",
            "$placeName 사장님이 정말 친절하시고 음식에 진심인 게 느껴져요"
        ).random()
        
        val sentence2 = listOf(
            "양도 푸짐하고 서비스도 친절해서 기분 좋게 식사할 수 있어요",
            "분위기도 좋고 음식도 정갈해서 모임하기 딱 좋은 장소예요",
            "정성스럽게 준비된 음식들이 입맛을 돋구고 시간 가는 줄 몰라요",
            "깔끔한 인테리어와 맛있는 음식이 조화롭게 어우러진 곳이에요"
        ).random()
        
        val sentence3 = if (!features.contains("일반적인")) {
            "$features 로 더욱 특별하고 기억에 남을 만한 곳이라고 생각해요"
        } else {
            "다음에 또 방문하고 싶을 정도로 전반적으로 만족스러운 식당이에요"
        }
        
        return "$sentence1. $sentence2. $sentence3."
    }
    
    private fun generateBakeryDescription(placeName: String, features: String, reviews: String): String {
        val variations = listOf(
            "$placeName 갓 구운 빵 냄새가 정말 좋아요. 식감도 부드럽고",
            "$placeName 여기 페스트리 정말 맛있어요. 커피랑 같이 먹으면 최고",
            "$placeName 아침 일찍 가면 따끈한 빵을 받을 수 있어요",
            "$placeName 종류도 다양하고 모든 빵이 정성스럽게 만들어져요",
            "$placeName 달달한 디저트부터 든든한 식사빵까지 다 있어요"
        )
        
        return variations.random() + if (!features.contains("일반적인")) " $features 도 큰 매력이에요" else ""
    }
    
    private fun generateBarDescription(placeName: String, features: String, reviews: String): String {
        val variations = listOf(
            "$placeName 분위기 정말 좋고 칵테일도 맛있어요. 데이트하기 딱",
            "$placeName 여기 바텐더분 실력이 정말 좋으세요. 안주도 괜찮고",
            "$placeName 조용히 술 한잔하기 좋은 곳이에요. 음악도 좋고",
            "$placeName 친구들이랑 와서 이야기하며 즐기기 좋은 공간이에요",
            "$placeName 분위기 있게 꾸며져 있고 드링크 종류도 다양해요"
        )
        
        return variations.random() + if (!features.contains("일반적인")) ". $features 으로 더욱 매력적이에요" else ""
    }
    
    private fun generateGenericDescription(placeName: String, category: String, features: String, reviews: String): String {
        val sentence1 = listOf(
            "$placeName 정말 괜찮은 곳이에요",
            "$placeName 여기 서비스 품질이 좋더라구요",
            "$placeName 한 번 가보시면 만족하실 거예요",
            "$placeName 이용하기 편리하고 접근성도 좋은 편이에요"
        ).random()
        
        val sentence2 = listOf(
            "직원분들도 친절하시고 시설도 깔끔하게 잘 관리되어 있어요",
            "전반적으로 관리가 잘 되어 있고 분위기도 좋은 편이에요",
            "가격도 합리적이고 이용하기에 부담스럽지 않은 곳이라고 생각해요",
            "시설이 깨끗하고 이용객들을 배려한 서비스가 인상적이에요"
        ).random()
        
        val sentence3 = if (!features.contains("일반적인")) {
            "특히 $features 부분이 다른 곳들과 차별화되는 매력적인 포인트예요"
        } else {
            "재방문 의사가 있을 정도로 전반적으로 만족스러운 경험을 제공하는 곳이에요"
        }
        
        return "$sentence1. $sentence2. $sentence3."
    }
    
    private fun generateGasStationDescription(placeName: String, features: String, reviews: String): String =
        "$placeName 접근성 좋은 주유소예요. 직원분들도 친절하고 부대시설도 깔끔해요"
    
    private fun generateMedicalDescription(placeName: String, features: String, reviews: String): String =
        "$placeName 시설 깔끔하고 직원분들 친절한 곳이에요. 대기시간도 적당하고 진료 받기 좋은 환경이에요"
    
    private fun generateBankDescription(placeName: String, features: String, reviews: String): String =
        "$placeName 업무 보기 편한 곳이에요. 직원분들 친절하고 시설도 깔끔해서 금융 업무 처리하기 좋아요"
    
    private fun generateFitnessDescription(placeName: String, features: String, reviews: String): String {
        val sentence1 = listOf(
            "$placeName 운동하기 정말 좋은 환경이에요",
            "$placeName 여기 시설이 깔끔하고 잘 관리되어 있어요",
            "$placeName 분위기가 좋아서 동기부여가 많이 돼요",
            "$placeName 장비도 다양하고 공간도 넓직해서 쾌적해요"
        ).random()
        
        val sentence2 = listOf(
            "직원분들도 친절하시고 운동 중 불편한 점이 있으면 바로 도와주세요",
            "시설도 잘 되어 있고 분위기도 좋아서 꾸준히 다니기 좋은 곳이에요",
            "다른 회원분들도 매너가 좋으시고 전체적으로 좋은 에너지가 느껴져요",
            "운동기구들이 최신식이고 청결하게 관리되어서 안심하고 이용할 수 있어요"
        ).random()
        
        val sentence3 = if (!features.contains("일반적인")) {
            "$features 덕분에 다른 곳들과는 차별화된 매력을 느낄 수 있는 곳이에요"
        } else {
            "건강한 라이프스타일을 유지하기에 정말 적합한 운동공간이라고 생각해요"
        }
        
        return "$sentence1. $sentence2. $sentence3."
    }
    
    private fun generateShoppingDescription(placeName: String, features: String, reviews: String): String =
        "$placeName 물건 구하기 편한 곳이에요. 종류도 다양하고 가격도 합리적이어서 자주 이용하게 돼요"
        
    private fun generateAccommodationDescription(placeName: String, features: String, reviews: String): String =
        "$placeName 깔끔하고 편안한 숙박시설이에요. 서비스도 좋고 위치도 괜찮아요"

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
        private var lastClearTime: Long = 0
        
        // Clear cache periodically to allow reprocessing
        fun clearCacheIfNeeded() {
            val currentTime = System.currentTimeMillis()
            if (currentTime - lastClearTime > 30000) { // Clear every 30 seconds
                processedPlaces.clear()
                lastClearTime = currentTime
                println("🔄 CLEARED PROCESSED PLACES CACHE - SIZE WAS: ${processedPlaces.size}")
            }
        }
    }
    
    private fun isRecentlyProcessed(identifier: String): Boolean {
        // Clear cache periodically to allow fresh data
        clearCacheIfNeeded()
        return false // TEMPORARILY DISABLE duplicate detection to allow all data through
    }
    
    private fun markAsProcessed(identifier: String) {
        clearCacheIfNeeded()
        processedPlaces.add(identifier)
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
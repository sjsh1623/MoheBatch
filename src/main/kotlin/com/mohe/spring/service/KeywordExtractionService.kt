package com.mohe.spring.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.mohe.spring.entity.PlaceKeywordExtraction
import com.mohe.spring.entity.SelectedKeyword
import com.mohe.spring.repository.PlaceKeywordExtractionRepository
import com.mohe.spring.repository.KeywordCatalogRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.util.retry.Retry
import java.security.MessageDigest
import java.time.Duration
import java.time.LocalDateTime
import jakarta.annotation.PostConstruct

data class KeywordSelectionResult(
    val selectedKeywords: List<SelectedKeyword>,
    val processingTimeMs: Long,
    val rawResponse: String,
    val vectorArray: FloatArray
)

@Service
class KeywordExtractionService(
    private val webClient: WebClient,
    private val placeKeywordExtractionRepository: PlaceKeywordExtractionRepository,
    private val keywordCatalogRepository: KeywordCatalogRepository,
    @Value("\${ollama.host}") private val ollamaHost: String,
    @Value("\${ollama.model}") private val ollamaModel: String,
    @Value("\${ollama.timeout:30}") private val ollamaTimeout: Int
) {
    private val logger = LoggerFactory.getLogger(KeywordExtractionService::class.java)
    private val objectMapper = ObjectMapper()
    
    private lateinit var availableKeywords: List<String>
    private lateinit var keywordDefinitions: Map<String, String>
    
    @PostConstruct
    fun initializeKeywordCatalog() {
        try {
            loadKeywordsFromDatabase()
            logger.info("Initialized keyword catalog with ${availableKeywords.size} keywords")
        } catch (e: Exception) {
            logger.error("Failed to initialize keyword catalog: ${e.message}", e)
            throw RuntimeException("Cannot initialize keyword extraction service", e)
        }
    }
    
    private fun loadKeywordsFromDatabase() {
        val catalogEntries = keywordCatalogRepository.findAllByOrderByVectorPosition()
        availableKeywords = catalogEntries.map { it.keyword }
        keywordDefinitions = catalogEntries.associate { it.keyword to it.definition }
        logger.info("Loaded ${availableKeywords.size} keywords from database catalog")
    }
    
    /**
     * Extract exactly 15 keywords from place description using Ollama
     */
    fun extractKeywords(
        placeId: Long,
        placeName: String, 
        placeDescription: String, 
        category: String,
        additionalContext: String = ""
    ): KeywordSelectionResult {
        val startTime = System.currentTimeMillis()
        
        try {
            val prompt = buildKeywordExtractionPrompt(
                placeName, placeDescription, category, additionalContext
            )
            
            val requestBody = mapOf(
                "model" to ollamaModel,
                "prompt" to prompt,
                "stream" to false,
                "options" to mapOf(
                    "temperature" to 0.3, // Lower temperature for more consistent keyword selection
                    "top_p" to 0.8,
                    "max_tokens" to 800 // Enough tokens for 15 keywords + reasoning
                )
            )

            val response = webClient.post()
                .uri("$ollamaHost/api/generate")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(Map::class.java)
                .retryWhen(
                    Retry.backoff(3, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofSeconds(15))
                        .filter { it is WebClientResponseException && it.statusCode.is5xxServerError }
                )
                .block(Duration.ofSeconds(ollamaTimeout.toLong()))

            val rawResponse = response?.get("response")?.toString()?.trim()
                ?: throw RuntimeException("Empty response from Ollama")

            val processingTime = System.currentTimeMillis() - startTime
            
            // Parse the response to extract keywords with confidence scores
            val selectedKeywords = parseKeywordResponse(rawResponse)
            
            // Validate exactly 15 keywords were selected
            if (selectedKeywords.size != 15) {
                logger.warn("Expected 15 keywords but got ${selectedKeywords.size} for place: $placeName")
            }
            
            // Calculate 100-dimensional vector
            val keywordVector = calculateKeywordVector(selectedKeywords)
            
            // Store in database
            saveKeywordExtraction(placeId, selectedKeywords, rawResponse, processingTime, keywordVector)
            
            return KeywordSelectionResult(
                selectedKeywords = selectedKeywords,
                processingTimeMs = processingTime,
                rawResponse = rawResponse,
                vectorArray = keywordVector
            )
            
        } catch (e: Exception) {
            val processingTime = System.currentTimeMillis() - startTime
            logger.error("Keyword extraction failed for place $placeName: ${e.message}", e)
            
            // Return fallback keywords based on category
            val fallbackKeywords = generateFallbackKeywords(category, placeDescription)
            val fallbackVector = calculateKeywordVector(fallbackKeywords)
            
            return KeywordSelectionResult(
                selectedKeywords = fallbackKeywords,
                processingTimeMs = processingTime,
                rawResponse = "FALLBACK: ${e.message}",
                vectorArray = fallbackVector
            )
        }
    }
    
    private fun buildKeywordExtractionPrompt(
        placeName: String, 
        placeDescription: String, 
        category: String,
        additionalContext: String
    ): String {
        val keywordList = availableKeywords.joinToString(", ")
        
        return """
당신은 장소 분류 전문가입니다. 다음 장소 정보를 분석하여 아래 100개 키워드 목록에서 정확히 15개의 키워드를 선택하고, 각각에 대해 0.0~1.0 사이의 신뢰도 점수를 부여해주세요.

장소 정보:
- 이름: $placeName
- 카테고리: $category  
- 설명: $placeDescription
${if (additionalContext.isNotEmpty()) "- 추가 정보: $additionalContext" else ""}

사용 가능한 키워드 (100개):
$keywordList

선택 기준:
1. 정확히 15개의 키워드만 선택
2. 장소의 특성을 가장 잘 설명하는 키워드 우선
3. 다양한 카테고리에서 균형있게 선택 (분위기, 음식/음료, 서비스, 활동, 위치, 가격)
4. 모순되는 키워드는 피하기 (예: quiet + lively)
5. 각 키워드의 신뢰도를 0.0~1.0으로 평가

출력 형식 (JSON):
{
  "selected_keywords": [
    {"keyword": "키워드1", "confidence_score": 0.9, "reasoning": "선택 이유"},
    {"keyword": "키워드2", "confidence_score": 0.8, "reasoning": "선택 이유"},
    ...15개
  ]
}

정확히 위 JSON 형식으로만 응답해주세요. 다른 설명이나 텍스트는 추가하지 마세요.
        """.trimIndent()
    }

    private fun calculateKeywordVector(selectedKeywords: List<SelectedKeyword>): FloatArray {
        val vector = FloatArray(100) { 0.0f }
        
        selectedKeywords.forEach { keywordWithConfidence ->
            // Get the vector position for this keyword from database
            val catalogEntry = keywordCatalogRepository.findByKeyword(keywordWithConfidence.keyword)
            catalogEntry?.let { entry ->
                val position = entry.vectorPosition
                if (position in 0..99) {
                    vector[position] = keywordWithConfidence.confidence.toFloat()
                }
            }
        }
        
        return vector
    }
    
    private fun parseKeywordResponse(rawResponse: String): List<SelectedKeyword> {
        return try {
            // Extract JSON from response if it contains other text
            val jsonStart = rawResponse.indexOf("{")
            val jsonEnd = rawResponse.lastIndexOf("}") + 1
            
            if (jsonStart == -1 || jsonEnd == 0) {
                throw IllegalArgumentException("No JSON found in response")
            }
            
            val jsonResponse = rawResponse.substring(jsonStart, jsonEnd)
            val responseMap = objectMapper.readValue<Map<String, Any>>(jsonResponse)
            
            @Suppress("UNCHECKED_CAST")
            val keywordsList = responseMap["selected_keywords"] as? List<Map<String, Any>>
                ?: throw IllegalArgumentException("Missing selected_keywords field")
            
            val keywords = keywordsList.mapNotNull { keywordMap ->
                val keyword = keywordMap["keyword"] as? String
                    ?: return@mapNotNull null
                val confidence = (keywordMap["confidence_score"] as? Number)?.toDouble()
                    ?: return@mapNotNull null
                val reasoning = keywordMap["reasoning"] as? String
                
                // Validate keyword exists in our catalog
                if (!availableKeywords.contains(keyword)) {
                    logger.warn("Unknown keyword '$keyword' returned by Ollama, skipping")
                    return@mapNotNull null
                }
                
                // Validate confidence score range
                val validConfidence = confidence.coerceIn(0.0, 1.0)
                if (validConfidence != confidence) {
                    logger.warn("Confidence score $confidence out of range, clamped to $validConfidence")
                }
                
                // Get keyword ID from catalog
                val catalogEntry = keywordCatalogRepository.findByKeyword(keyword)
                val keywordId = catalogEntry?.vectorPosition ?: -1
                
                if (keywordId == -1) {
                    logger.warn("Could not find vector position for keyword: $keyword")
                    return@mapNotNull null
                }
                
                SelectedKeyword(keywordId, keyword, validConfidence, reasoning)
            }
            
            // Ensure we have exactly 15 keywords, pad with fallbacks if needed
            if (keywords.size < 15) {
                logger.warn("Only ${keywords.size} valid keywords parsed, padding with fallbacks")
                val existing = keywords.map { it.keyword }.toSet()
                val fallbacks = generateFallbackKeywords("", "").take(15 - keywords.size)
                    .filter { !existing.contains(it.keyword) }
                
                return keywords + fallbacks
            }
            
            keywords.take(15) // Ensure exactly 15
            
        } catch (e: Exception) {
            logger.error("Failed to parse keyword response: ${e.message}, raw response: $rawResponse", e)
            generateFallbackKeywords("", "").take(15)
        }
    }
    
    private fun generateFallbackKeywords(category: String, description: String): List<SelectedKeyword> {
        // Generate basic fallback keywords based on category and common characteristics
        val fallbacks = mutableListOf<SelectedKeyword>()
        
        when {
            category.contains("카페", ignoreCase = true) -> {
                fallbacks.addAll(listOf(
                    SelectedKeyword(10, "coffee", 0.8, "Category-based fallback"),
                    SelectedKeyword(0, "cozy", 0.7, "Common cafe characteristic"),
                    SelectedKeyword(20, "wifi", 0.6, "Common cafe amenity")
                ))
            }
            category.contains("음식", ignoreCase = true) || category.contains("레스토랑", ignoreCase = true) -> {
                fallbacks.addAll(listOf(
                    SelectedKeyword(65, "casual", 0.7, "Category-based fallback"),
                    SelectedKeyword(82, "local_cuisine", 0.6, "Common restaurant type"),
                    SelectedKeyword(37, "socializing", 0.5, "Common activity")
                ))
            }
            else -> {
                fallbacks.addAll(listOf(
                    SelectedKeyword(93, "popular_spot", 0.5, "Generic fallback"),
                    SelectedKeyword(95, "central_location", 0.5, "Generic characteristic")
                ))
            }
        }
        
        // Add generic keywords to reach 15
        val genericKeywords = listOf(
            Pair(40, "downtown"), Pair(41, "subway_nearby"), Pair(50, "affordable"), 
            Pair(52, "mid_range"), Pair(84, "attentive_staff"), Pair(66, "upscale"),
            Pair(90, "tourist_friendly"), Pair(53, "value_for_money"), Pair(95, "central_location"),
            Pair(98, "authentic"), Pair(91, "locals_favorite"), Pair(94, "scenic_view")
        )
        
        genericKeywords.forEach { (keywordId, keyword) ->
            if (fallbacks.size < 15 && availableKeywords.contains(keyword)) {
                fallbacks.add(SelectedKeyword(keywordId, keyword, 0.4, "Generic fallback"))
            }
        }
        
        return fallbacks.take(15)
    }

    private fun saveKeywordExtraction(
        placeId: Long, 
        selectedKeywords: List<SelectedKeyword>, 
        rawResponse: String, 
        processingTimeMs: Long,
        keywordVector: FloatArray
    ) {
        try {
            // Convert to JSONB format for database
            val keywordsJson = selectedKeywords.map { 
                mapOf(
                    "keyword" to it.keyword,
                    "confidence_score" to it.confidence,
                    "reasoning" to it.reasoning
                )
            }
            
            val extraction = PlaceKeywordExtraction(
                placeId = placeId,
                rawText = rawResponse.take(1000), // Limit raw text length
                modelName = "ollama",
                modelVersion = ollamaModel,
                keywordVector = keywordVector,
                selectedKeywords = objectMapper.writeValueAsString(keywordsJson),
                processingTimeMs = processingTimeMs.toInt(),
                createdAt = LocalDateTime.now()
            )
            
            placeKeywordExtractionRepository.save(extraction)
            logger.debug("Saved keyword extraction for place $placeId with ${selectedKeywords.size} keywords")
            
        } catch (e: Exception) {
            logger.error("Failed to save keyword extraction for place $placeId: ${e.message}", e)
        }
    }

    fun generatePromptHash(placeName: String, placeDescription: String, category: String): String {
        val prompt = buildKeywordExtractionPrompt(placeName, placeDescription, category, "")
        return MessageDigest.getInstance("SHA-256")
            .digest(prompt.toByteArray())
            .fold("") { str, it -> str + "%02x".format(it) }
    }
    
    /**
     * Find similar places based on keyword vector similarity
     */
    fun findSimilarPlacesByKeywords(placeId: Long, limit: Int = 10): List<Long> {
        return try {
            placeKeywordExtractionRepository.findSimilarPlacesByVector(placeId, limit)
        } catch (e: Exception) {
            logger.error("Failed to find similar places for $placeId: ${e.message}", e)
            emptyList()
        }
    }

    /**
     * Parse Ollama response to extract keywords and confidences
     */
    private fun parseKeywordResponse(responseText: String): List<SelectedKeyword> {
        try {
            // Try to parse as JSON array first
            val jsonStart = responseText.indexOf('[')
            val jsonEnd = responseText.lastIndexOf(']') + 1
            
            if (jsonStart >= 0 && jsonEnd > jsonStart) {
                val jsonText = responseText.substring(jsonStart, jsonEnd)
                val jsonNode = jsonMapper.readTree(jsonText)
                
                val keywords = mutableListOf<SelectedKeyword>()
                jsonNode.forEach { node ->
                    val keywordName = node["keyword"]?.asText() ?: ""
                    val confidence = node["confidence"]?.asDouble() ?: 0.0
                    
                    // Map keyword name to ID from catalog
                    val keywordDef = keywords.find { it.keyword == keywordName }
                    if (keywordDef != null && confidence >= extractionConfig.minConfidence) {
                        keywords.add(
                            SelectedKeyword(
                                keywordId = keywordDef.id,
                                keyword = keywordName,
                                confidence = confidence
                            )
                        )
                    }
                }
                
                return keywords.take(extractionConfig.maxKeywords)
            }
            
        } catch (e: Exception) {
            logger.warn("Failed to parse JSON response, trying fallback parsing", e)
        }
        
        // Fallback: try to extract keywords from text
        return parseKeywordResponseFallback(responseText)
    }
    
    /**
     * Fallback parser for non-JSON responses
     */
    private fun parseKeywordResponseFallback(responseText: String): List<SelectedKeyword> {
        val keywords = mutableListOf<SelectedKeyword>()
        val keywordMap = keywords.associateBy { it.keyword.lowercase() }
        
        // Extract keyword-like patterns with confidence scores
        val patterns = listOf(
            Regex("""(\w+(?:_\w+)*)\s*[:\-]\s*(0\.\d+|\d\.\d+)"""), // keyword: 0.85
            Regex("""(\w+(?:_\w+)*)\s*\(([0-9.]+)\)"""), // keyword(0.85)
            Regex(""""(\w+(?:_\w+)*)"\s*:\s*([0-9.]+)""") // "keyword": 0.85
        )
        
        for (pattern in patterns) {
            val matches = pattern.findAll(responseText.lowercase())
            for (match in matches) {
                val keywordName = match.groupValues[1]
                val confidence = match.groupValues[2].toDoubleOrNull() ?: 0.0
                
                val keywordDef = keywordMap[keywordName]
                if (keywordDef != null && confidence >= extractionConfig.minConfidence) {
                    keywords.add(
                        SelectedKeyword(
                            keywordId = keywordDef.id,
                            keyword = keywordDef.keyword,
                            confidence = confidence
                        )
                    )
                    
                    if (keywords.size >= extractionConfig.maxKeywords) break
                }
            }
            if (keywords.size >= extractionConfig.maxKeywords) break
        }
        
        return keywords
    }
    
    /**
     * Create fallback extraction using rule-based approach
     */
    private fun createFallbackExtraction(
        text: String, 
        contextType: String,
        userMbti: String?
    ): KeywordExtractionResult {
        
        val normalizedText = preprocessText(text)
        val fallbackKeywords = extractKeywordsRuleBased(normalizedText, contextType, userMbti)
        val vectorResult = createVector(fallbackKeywords)
        
        return KeywordExtractionResult(
            originalText = text,
            normalizedText = normalizedText,
            selectedKeywords = fallbackKeywords,
            vector = vectorResult.vector,
            extractionSource = "rule-based-fallback",
            modelName = "internal",
            promptHash = generatePromptHash(text),
            confidence = 0.6 // Lower confidence for rule-based
        )
    }
    
    /**
     * Rule-based keyword extraction as fallback
     */
    private fun extractKeywordsRuleBased(
        text: String,
        contextType: String,
        userMbti: String?
    ): List<SelectedKeyword> {
        
        val selectedKeywords = mutableListOf<SelectedKeyword>()
        val textLower = text.lowercase()
        
        // Simple keyword matching based on text content
        keywords.forEach { keywordDef ->
            val matchScore = calculateKeywordMatchScore(textLower, keywordDef)
            if (matchScore >= extractionConfig.minConfidence) {
                selectedKeywords.add(
                    SelectedKeyword(
                        keywordId = keywordDef.id,
                        keyword = keywordDef.keyword,
                        confidence = matchScore
                    )
                )
            }
        }
        
        // Sort by confidence and take top 15
        return selectedKeywords
            .sortedByDescending { it.confidence }
            .take(extractionConfig.maxKeywords)
    }
    
    /**
     * Calculate match score for rule-based extraction
     */
    private fun calculateKeywordMatchScore(text: String, keywordDef: KeywordDefinition): Double {
        var score = 0.0
        val keyword = keywordDef.keyword.replace("_", " ")
        
        // Direct keyword match
        if (text.contains(keyword)) score += 0.8
        
        // Partial matches and synonyms based on keyword definition
        when (keywordDef.keyword) {
            "specialty_coffee" -> if (text.contains("coffee") || text.contains("espresso") || text.contains("latte")) score += 0.7
            "quiet_space" -> if (text.contains("quiet") || text.contains("peaceful") || text.contains("study")) score += 0.8
            "free_wifi" -> if (text.contains("wifi") || text.contains("internet") || text.contains("connection")) score += 0.9
            // Add more keyword-specific matching rules as needed
        }
        
        return (score * keywordDef.weightBoost).coerceAtMost(1.0)
    }
    
    /**
     * Build extraction prompt for Ollama
     */
    private fun buildExtractionPrompt(text: String, contextType: String, userMbti: String?): String {
        val keywordList = keywords.joinToString("\n") { "${it.id}. ${it.keyword} - ${it.definition}" }
        
        val mbtiContext = userMbti?.let { 
            "\nUser MBTI Type: $it (consider MBTI-specific preferences in keyword selection)"
        } ?: ""
        
        return extractionConfig.promptTemplate
            .replace("{text}", text)
            .replace("{keyword_list}", keywordList) + mbtiContext
    }
    
    /**
     * Preprocess text for extraction
     */
    private fun preprocessText(text: String): String {
        return text.trim()
            .replace(Regex("\\s+"), " ") // Normalize whitespace
            .replace(Regex("[^\\w\\s가-힣]"), " ") // Keep only words, spaces, Korean
            .take(2000) // Limit length for API efficiency
    }
    
    /**
     * Calculate overall confidence from selected keywords
     */
    private fun calculateOverallConfidence(keywords: List<SelectedKeyword>): Double {
        if (keywords.isEmpty()) return 0.0
        return keywords.map { it.confidence }.average()
    }
    
    /**
     * Generate SHA-256 hash of prompt for caching
     */
    private fun generatePromptHash(prompt: String): String {
        return MessageDigest.getInstance("SHA-256")
            .digest(prompt.toByteArray())
            .fold("") { str, it -> str + "%02x".format(it) }
    }
    
    /**
     * Parse keywords from YAML configuration
     */
    private fun parseKeywords(): List<KeywordDefinition> {
        val keywordsList = mutableListOf<KeywordDefinition>()
        val keywordsSection = keywordCatalog["keywords"] as? Map<String, Any> ?: return keywordsList
        
        keywordsSection.values.forEach { categoryData ->
            if (categoryData is List<*>) {
                categoryData.forEach { keywordData ->
                    if (keywordData is Map<*, *>) {
                        keywordsList.add(
                            KeywordDefinition(
                                id = (keywordData["id"] as? Number)?.toInt() ?: 0,
                                keyword = keywordData["keyword"] as? String ?: "",
                                definition = keywordData["definition"] as? String ?: "",
                                category = keywordData["category"] as? String ?: "",
                                weightBoost = (keywordData["weight_boost"] as? Number)?.toDouble() ?: 1.0
                            )
                        )
                    }
                }
            }
        }
        
        return keywordsList.sortedBy { it.id }
    }
    
    /**
     * Parse extraction configuration
     */
    private fun parseExtractionConfig(): ExtractionConfig {
        val extractionSection = keywordCatalog["extraction"] as? Map<String, Any> ?: emptyMap()
        
        return ExtractionConfig(
            maxKeywords = (extractionSection["max_keywords"] as? Number)?.toInt() ?: 15,
            minConfidence = (extractionSection["min_confidence"] as? Number)?.toDouble() ?: 0.1,
            modelName = extractionSection["model_name"] as? String ?: "ollama-openai",
            promptTemplate = extractionSection["prompt_template"] as? String ?: "Extract keywords from: {text}"
        )
    }
}

/**
 * Data classes for keyword extraction
 */
data class KeywordDefinition(
    val id: Int,
    val keyword: String,
    val definition: String,
    val category: String,
    val weightBoost: Double
)

data class ExtractionConfig(
    val maxKeywords: Int,
    val minConfidence: Double,
    val modelName: String,
    val promptTemplate: String
)

data class KeywordExtractionResult(
    val originalText: String,
    val normalizedText: String,
    val selectedKeywords: List<SelectedKeyword>,
    val vector: FloatArray,
    val extractionSource: String,
    val modelName: String,
    val promptHash: String,
    val confidence: Double
) {
    fun getVectorAsString(): String {
        return "[${vector.joinToString(",")}]"
    }
    
    fun getSelectedKeywordsAsJson(): String {
        return ObjectMapper().writeValueAsString(selectedKeywords)
    }
}

data class VectorResult(
    val vector: FloatArray,
    val vectorString: String,
    val nonZeroCount: Int,
    val magnitude: Float
)
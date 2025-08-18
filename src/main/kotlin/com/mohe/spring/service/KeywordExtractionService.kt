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
    @Value("\${ollama.text.model}") private val ollamaModel: String,
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
You are a place classification expert. Please analyze the following place information and select exactly 15 keywords from the 100-keyword list below, assigning a confidence score between 0.0 and 1.0 for each.

Place Information:
- Name: $placeName
- Category: $category  
- Description: $placeDescription
${if (additionalContext.isNotEmpty()) "- Additional Context: $additionalContext" else ""}

Available Keywords (100 total):
$keywordList

Selection Criteria:
1. Select exactly 15 keywords only
2. Prioritize keywords that best describe the place characteristics
3. Select balanced keywords from different categories (atmosphere, food/beverage, service, activities, location, price)
4. Avoid contradictory keywords (e.g., quiet + lively)
5. Rate each keyword's confidence from 0.0 to 1.0

Output Format (JSON):
{
  "selected_keywords": [
    {"keyword": "keyword1", "confidence_score": 0.9, "reasoning": "selection reason"},
    {"keyword": "keyword2", "confidence_score": 0.8, "reasoning": "selection reason"},
    ...15 total
  ]
}

Please respond only in the exact JSON format above. Do not add any other explanations or text.
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

}

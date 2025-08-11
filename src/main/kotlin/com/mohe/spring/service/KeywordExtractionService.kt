package com.mohe.spring.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.mohe.spring.entity.SelectedKeyword
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.util.retry.Retry
import java.security.MessageDigest
import java.time.Duration
import jakarta.annotation.PostConstruct

@Service
class KeywordExtractionService(
    private val webClient: WebClient,
    @Value("\${ollama.host}") private val ollamaHost: String,
    @Value("\${ollama.model}") private val ollamaModel: String,
    @Value("\${ollama.timeout:30}") private val ollamaTimeout: Int
) {
    private val logger = LoggerFactory.getLogger(KeywordExtractionService::class.java)
    private val objectMapper = ObjectMapper(YAMLFactory())
    private val jsonMapper = ObjectMapper()
    
    // Loaded from keyword-catalog.yml
    private lateinit var keywordCatalog: Map<String, Any>
    private lateinit var keywords: List<KeywordDefinition>
    private lateinit var extractionConfig: ExtractionConfig
    
    @PostConstruct
    private fun loadKeywordCatalog() {
        try {
            val resource = ClassPathResource("keyword-catalog.yml")
            keywordCatalog = objectMapper.readValue(resource.inputStream, Map::class.java) as Map<String, Any>
            
            // Parse keywords from all categories
            keywords = parseKeywords()
            extractionConfig = parseExtractionConfig()
            
            logger.info("Loaded ${keywords.size} keywords from catalog")
            
        } catch (e: Exception) {
            logger.error("Failed to load keyword catalog", e)
            throw RuntimeException("Cannot initialize keyword extraction service", e)
        }
    }
    
    /**
     * Extract exactly 15 keywords from text using Ollama
     */
    fun extractKeywords(
        text: String,
        contextType: String = "place", // "place" or "user"
        userMbti: String? = null
    ): KeywordExtractionResult {
        
        val normalizedText = preprocessText(text)
        val prompt = buildExtractionPrompt(normalizedText, contextType, userMbti)
        val promptHash = generatePromptHash(prompt)
        
        try {
            logger.debug("Extracting keywords for text: ${normalizedText.take(100)}...")
            
            val extractedKeywords = callOllamaForExtraction(prompt)
            val vectorizedResult = createVector(extractedKeywords)
            
            return KeywordExtractionResult(
                originalText = text,
                normalizedText = normalizedText,
                selectedKeywords = extractedKeywords,
                vector = vectorizedResult.vector,
                extractionSource = "ollama-openai",
                modelName = ollamaModel,
                promptHash = promptHash,
                confidence = calculateOverallConfidence(extractedKeywords)
            )
            
        } catch (e: Exception) {
            logger.error("Failed to extract keywords from text: ${text.take(100)}...", e)
            // Fallback to rule-based extraction
            return createFallbackExtraction(text, contextType, userMbti)
        }
    }
    
    /**
     * Create a 100-dimensional vector from selected keywords
     */
    private fun createVector(selectedKeywords: List<SelectedKeyword>): VectorResult {
        val vector = FloatArray(100) { 0.0f }
        
        selectedKeywords.forEach { keyword ->
            val keywordDef = keywords.find { it.id == keyword.keywordId }
            if (keywordDef != null && keyword.keywordId in 1..100) {
                val vectorIndex = keyword.keywordId - 1 // Convert to 0-based index
                val weightedValue = (keyword.confidence * keywordDef.weightBoost).toFloat()
                vector[vectorIndex] = weightedValue.coerceAtMost(1.0f)
            }
        }
        
        // Normalize vector to unit length for cosine similarity
        val magnitude = kotlin.math.sqrt(vector.map { it * it }.sum())
        if (magnitude > 0) {
            for (i in vector.indices) {
                vector[i] = vector[i] / magnitude.toFloat()
            }
        }
        
        return VectorResult(
            vector = vector,
            vectorString = "[${vector.joinToString(",")}]",
            nonZeroCount = vector.count { it != 0.0f },
            magnitude = magnitude.toFloat()
        )
    }
    
    /**
     * Call Ollama API for keyword extraction
     */
    private fun callOllamaForExtraction(prompt: String): List<SelectedKeyword> {
        val requestBody = mapOf(
            "model" to ollamaModel,
            "prompt" to prompt,
            "stream" to false,
            "options" to mapOf(
                "temperature" to 0.3, // Lower temperature for more consistent extraction
                "top_p" to 0.8,
                "max_tokens" to 500,
                "stop" to listOf("\n\n") // Stop at double newline
            )
        )

        val response = webClient.post()
            .uri("$ollamaHost/api/generate")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(requestBody)
            .retrieve()
            .bodyToMono(Map::class.java)
            .retryWhen(
                Retry.backoff(3, Duration.ofSeconds(1))
                    .maxBackoff(Duration.ofSeconds(10))
                    .filter { it is WebClientResponseException && it.statusCode.is5xxServerError }
            )
            .block(Duration.ofSeconds(ollamaTimeout.toLong()))

        val responseText = response?.get("response")?.toString()?.trim()
            ?: throw RuntimeException("Empty response from Ollama")

        return parseKeywordResponse(responseText)
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
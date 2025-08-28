package com.mohe.spring.service

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.util.retry.Retry
import java.security.MessageDigest
import java.time.Duration

@Service
class OllamaService(
    private val webClient: WebClient,
    @Value("\${ollama.host}") private val ollamaHost: String,
    @Value("\${ollama.text.model}") private val ollamaModel: String,
    @Value("\${ollama.timeout:30}") private val ollamaTimeout: Int
) {
    private val logger = LoggerFactory.getLogger(OllamaService::class.java)

    private val mbtiTypes = listOf(
        "INTJ", "INTP", "ENTJ", "ENTP",
        "INFJ", "INFP", "ENFJ", "ENFP", 
        "ISTJ", "ISFJ", "ESTJ", "ESFJ",
        "ISTP", "ISFP", "ESTP", "ESFP"
    )

    fun generateMbtiDescriptions(placeName: String, placeDescription: String, category: String): Map<String, String> {
        val results = mutableMapOf<String, String>()
        
        mbtiTypes.forEach { mbti ->
            try {
                val description = generateMbtiDescription(placeName, placeDescription, category, mbti)
                results[mbti] = description
                logger.debug("Generated MBTI description for $placeName ($mbti): ${description.take(100)}...")
            } catch (e: Exception) {
                logger.error("Failed to generate MBTI description for $placeName ($mbti): ${e.message}", e)
                // Add fallback description
                results[mbti] = generateFallbackDescription(placeName, category, mbti)
            }
        }
        
        return results
    }

    private fun generateMbtiDescription(placeName: String, placeDescription: String, category: String, mbti: String): String {
        val prompt = buildMbtiPrompt(placeName, placeDescription, category, mbti)
        
        val requestBody = mapOf(
            "model" to ollamaModel,
            "prompt" to prompt,
            "stream" to false,
            "options" to mapOf(
                "temperature" to 0.7,
                "top_p" to 0.9,
                "max_tokens" to 150
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

        return response?.get("response")?.toString()?.trim() 
            ?: throw RuntimeException("Empty response from Ollama")
    }

    private fun buildMbtiPrompt(placeName: String, placeDescription: String, category: String, mbti: String): String {
        val mbtiTraits = getMbtiTraits(mbti)
        
        return """
You are an MBTI expert. Please explain in about 100 characters why people with ${mbti} personality type would like this place.

Place Name: $placeName
Category: $category  
Description: $placeDescription

${mbti} Characteristics: $mbtiTraits

Please specifically describe how this place would be appealing to people with ${mbti} personality type. Write from an emotional and personal perspective.
        """.trimIndent()
    }

    private fun getMbtiTraits(mbti: String): String {
        return when (mbti) {
            "INTJ" -> "Independent and strategic thinkers who prefer quiet, focused environments"
            "INTP" -> "Enjoy logical analysis and prefer flexible spaces that allow free thinking"
            "ENTJ" -> "Goal-oriented with strong leadership, value efficient and high-quality service"
            "ENTP" -> "Creative and seek new experiences, enjoy networking with diverse people"
            "INFJ" -> "Seek deep conversations and meaningful experiences, prefer quiet and cozy atmospheres"
            "INFP" -> "Value personal authenticity, love creative and inspiring spaces"
            "ENFJ" -> "Value human connections, prefer warm and inclusive atmosphere places"
            "ENFP" -> "Enthusiastic and social, enjoy lively places with diverse activities"
            "ISTJ" -> "Value tradition and stability, prefer orderly and reliable service"
            "ISFJ" -> "Warm and caring, like comfortable and friendly atmosphere places"
            "ESTJ" -> "Practical and organized, value efficient and professional service"
            "ESFJ" -> "Social and cooperative, prefer places where they can enjoy with others"
            "ISTP" -> "Practical and independent, like free environments with few constraints"
            "ISFP" -> "Artistically sensitive, prefer beautiful and peaceful atmosphere places"
            "ESTP" -> "Active and realistic, enjoy dynamic and spontaneous experiences"
            "ESFP" -> "Social and fun-seeking, like lively and entertaining atmosphere places"
            else -> "Places that respect individual personality and diverse preferences"
        }
    }

    private fun generateFallbackDescription(placeName: String, category: String, mbti: String): String {
        val categoryDesc = when {
            category.contains("cafe", ignoreCase = true) || category.contains("카페") -> "cafe"
            category.contains("food", ignoreCase = true) || category.contains("restaurant", ignoreCase = true) || category.contains("음식") || category.contains("레스토랑") -> "restaurant"
            category.contains("culture", ignoreCase = true) || category.contains("museum", ignoreCase = true) || category.contains("문화") || category.contains("박물관") -> "cultural space"
            else -> "place"
        }
        
        val mbtiDesc = when (mbti.first()) {
            'I' -> "quiet and personal"
            'E' -> "social and vibrant"
            else -> "comfortable and pleasant"
        }
        
        return "This is a $mbtiDesc $categoryDesc that suits ${mbti} personality type well."
    }

    fun generatePromptHash(placeName: String, placeDescription: String, category: String, mbti: String): String {
        val prompt = buildMbtiPrompt(placeName, placeDescription, category, mbti)
        return MessageDigest.getInstance("SHA-256")
            .digest(prompt.toByteArray())
            .fold("") { str, it -> str + "%02x".format(it) }
    }

    /**
     * Extract normalized keywords/intents from user query for vector search
     */
    fun extractKeywordsFromQuery(query: String, weatherContext: String? = null, timeContext: String? = null): List<String> {
        return try {
            val prompt = buildKeywordExtractionPrompt(query, weatherContext, timeContext)
            
            val requestBody = mapOf(
                "model" to ollamaModel,
                "prompt" to prompt,
                "stream" to false,
                "options" to mapOf(
                    "temperature" to 0.3, // Lower temperature for more consistent extraction
                    "top_p" to 0.9,
                    "max_tokens" to 100
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

            val responseText = response?.get("response")?.toString()?.trim() ?: ""
            parseKeywords(responseText, query)
            
        } catch (e: Exception) {
            logger.error("Failed to extract keywords from query: $query", e)
            // Fallback: simple keyword extraction
            extractFallbackKeywords(query)
        }
    }

    /**
     * Generate suitability tags for place based on weather and time
     */
    fun inferPlaceSuitability(
        placeName: String, 
        description: String, 
        category: String,
        openingHours: String? = null
    ): Map<String, Any> {
        return try {
            val prompt = buildSuitabilityPrompt(placeName, description, category, openingHours)
            
            val requestBody = mapOf(
                "model" to ollamaModel,
                "prompt" to prompt,
                "stream" to false,
                "options" to mapOf(
                    "temperature" to 0.5,
                    "top_p" to 0.9,
                    "max_tokens" to 200
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

            val responseText = response?.get("response")?.toString()?.trim() ?: ""
            parseSuitabilityResponse(responseText, category)
            
        } catch (e: Exception) {
            logger.error("Failed to infer place suitability for: $placeName", e)
            // Fallback suitability based on category
            generateFallbackSuitability(category)
        }
    }

    private fun buildKeywordExtractionPrompt(query: String, weatherContext: String?, timeContext: String?): String {
        val contextInfo = listOfNotNull(weatherContext, timeContext).joinToString(", ")
        val contextSuffix = if (contextInfo.isNotEmpty()) "\nContext: $contextInfo" else ""
        
        return """
Extract 3-5 normalized keywords from the user's search query for place recommendations.
Focus on: place type, activity, mood, atmosphere, specific features.
Return only the keywords separated by commas, no explanations.

Query: "$query"$contextSuffix

Keywords:
        """.trimIndent()
    }

    private fun buildSuitabilityPrompt(placeName: String, description: String, category: String, openingHours: String?): String {
        val hoursInfo = if (openingHours != null) "\nHours: $openingHours" else ""
        
        return """
Analyze this place and determine its suitability for different weather and time conditions.
Respond with JSON format only.

Place: $placeName
Category: $category
Description: $description$hoursInfo

Return JSON with these keys:
- suitable_weather: ["clear", "rainy", "cold", "hot"] (array of suitable weather conditions)
- suitable_time: ["morning", "afternoon", "evening", "night"] (array of suitable times)
- indoor: true/false (is it primarily an indoor place)
- outdoor: true/false (is it primarily an outdoor place)

JSON:
        """.trimIndent()
    }

    private fun parseKeywords(responseText: String, originalQuery: String): List<String> {
        // Try to extract comma-separated keywords
        val keywords = responseText.split(",", "\n", ";")
            .map { it.trim().lowercase() }
            .filter { it.isNotBlank() && it.length > 2 }
            .distinct()
            .take(5)
        
        // If parsing failed, use fallback
        return if (keywords.isNotEmpty()) keywords else extractFallbackKeywords(originalQuery)
    }

    private fun extractFallbackKeywords(query: String): List<String> {
        val commonPlaceTypes = mapOf(
            "카페" to listOf("cafe", "coffee", "study", "quiet"),
            "레스토랑" to listOf("restaurant", "food", "dining", "meal"),
            "음식" to listOf("food", "eat", "taste", "meal"),
            "공원" to listOf("park", "outdoor", "nature", "walk"),
            "박물관" to listOf("museum", "culture", "art", "indoor"),
            "쇼핑" to listOf("shopping", "store", "mall", "buy")
        )
        
        val queryLower = query.lowercase()
        val foundKeywords = commonPlaceTypes.entries
            .filter { (key, _) -> queryLower.contains(key) }
            .flatMap { (_, value) -> value }
            .distinct()
        
        return if (foundKeywords.isNotEmpty()) {
            foundKeywords.take(5)
        } else {
            // Extract words from query
            query.split(" ", ",", ".", "!")
                .map { it.trim().lowercase() }
                .filter { it.length > 2 }
                .distinct()
                .take(3)
        }
    }

    private fun parseSuitabilityResponse(responseText: String, category: String): Map<String, Any> {
        return try {
            // Try to find JSON-like content
            val jsonStart = responseText.indexOf("{")
            val jsonEnd = responseText.lastIndexOf("}") + 1
            
            if (jsonStart >= 0 && jsonEnd > jsonStart) {
                val jsonText = responseText.substring(jsonStart, jsonEnd)
                val objectMapper = com.fasterxml.jackson.databind.ObjectMapper()
                objectMapper.readValue(jsonText, Map::class.java) as Map<String, Any>
            } else {
                generateFallbackSuitability(category)
            }
        } catch (e: Exception) {
            logger.warn("Failed to parse suitability JSON response: $responseText", e)
            generateFallbackSuitability(category)
        }
    }

    private fun generateFallbackSuitability(category: String): Map<String, Any> {
        val categoryLower = category.lowercase()
        
        return when {
            categoryLower.contains("카페") || categoryLower.contains("cafe") -> mapOf(
                "suitable_weather" to listOf("clear", "rainy", "cold"),
                "suitable_time" to listOf("morning", "afternoon", "evening"),
                "indoor" to true,
                "outdoor" to false
            )
            categoryLower.contains("공원") || categoryLower.contains("park") -> mapOf(
                "suitable_weather" to listOf("clear", "hot"),
                "suitable_time" to listOf("morning", "afternoon", "evening"),
                "indoor" to false,
                "outdoor" to true
            )
            categoryLower.contains("박물관") || categoryLower.contains("museum") -> mapOf(
                "suitable_weather" to listOf("clear", "rainy", "cold", "hot"),
                "suitable_time" to listOf("morning", "afternoon", "evening"),
                "indoor" to true,
                "outdoor" to false
            )
            categoryLower.contains("레스토랑") || categoryLower.contains("restaurant") -> mapOf(
                "suitable_weather" to listOf("clear", "rainy", "cold", "hot"),
                "suitable_time" to listOf("morning", "afternoon", "evening", "night"),
                "indoor" to true,
                "outdoor" to false
            )
            else -> mapOf(
                "suitable_weather" to listOf("clear", "rainy"),
                "suitable_time" to listOf("morning", "afternoon", "evening"),
                "indoor" to true,
                "outdoor" to false
            )
        }
    }
}
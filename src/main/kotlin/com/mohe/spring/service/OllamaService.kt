package com.mohe.spring.service

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
}
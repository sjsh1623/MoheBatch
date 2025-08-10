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
    @Value("\${ollama.model}") private val ollamaModel: String,
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
당신은 MBTI 전문가입니다. 다음 장소에 대해 ${mbti} 성격의 사람들이 좋아할 만한 이유를 한국어로 100자 내외로 설명해주세요.

장소명: $placeName
카테고리: $category  
장소 설명: $placeDescription

${mbti} 특징: $mbtiTraits

${mbti} 성격의 사람들에게 이 장소가 어떻게 매력적인지 구체적으로 설명해주세요. 감정적이고 개인적인 관점에서 작성해주세요.
        """.trimIndent()
    }

    private fun getMbtiTraits(mbti: String): String {
        return when (mbti) {
            "INTJ" -> "독립적이고 전략적 사고를 선호하며, 조용하고 집중할 수 있는 환경을 좋아함"
            "INTP" -> "논리적 분석을 즐기고, 자유로운 사고가 가능한 유연한 공간을 선호함"
            "ENTJ" -> "목표 지향적이고 리더십이 강하며, 효율적이고 품질 높은 서비스를 중시함"
            "ENTP" -> "창의적이고 새로운 경험을 추구하며, 다양한 사람들과의 네트워킹을 즐김"
            "INFJ" -> "깊이 있는 대화와 의미 있는 경험을 추구하며, 조용하고 아늑한 분위기를 선호함"
            "INFP" -> "개인적 가치와 진정성을 중시하며, 창의적이고 영감을 주는 공간을 좋아함"
            "ENFJ" -> "사람들과의 연결을 중요시하고, 따뜻하고 포용적인 분위기의 장소를 선호함"
            "ENFP" -> "열정적이고 사교적이며, 활기차고 다양한 활동이 가능한 장소를 즐김"
            "ISTJ" -> "전통과 안정성을 중시하며, 질서정연하고 신뢰할 수 있는 서비스를 선호함"
            "ISFJ" -> "따뜻하고 배려심 깊으며, 편안하고 친근한 분위기의 장소를 좋아함"
            "ESTJ" -> "실용적이고 조직적이며, 효율적이고 전문적인 서비스를 중시함"
            "ESFJ" -> "사교적이고 협조적이며, 사람들과 함께 즐길 수 있는 장소를 선호함"
            "ISTP" -> "실용적이고 독립적이며, 자유롭고 제약이 적은 환경을 좋아함"
            "ISFP" -> "예술적 감성이 풍부하고, 아름답고 평화로운 분위기의 장소를 선호함"
            "ESTP" -> "활동적이고 현실적이며, 역동적이고 즉흥적인 경험을 즐김"
            "ESFP" -> "사교적이고 즐거움을 추구하며, 활기차고 재미있는 분위기의 장소를 좋아함"
            else -> "각자의 개성과 선호를 존중하는 다양성이 있는 장소"
        }
    }

    private fun generateFallbackDescription(placeName: String, category: String, mbti: String): String {
        val categoryDesc = when {
            category.contains("카페") -> "카페"
            category.contains("음식") || category.contains("레스토랑") -> "음식점"
            category.contains("문화") || category.contains("박물관") -> "문화공간"
            else -> "장소"
        }
        
        val mbtiDesc = when (mbti.first()) {
            'I' -> "조용하고 개인적인 시간을 즐길 수 있는"
            'E' -> "사람들과 어울리고 활기찬 분위기를 즐길 수 있는"
            else -> "편안하고 좋은"
        }
        
        return "$mbtiDesc $categoryDesc 로 ${mbti} 성격에 잘 맞는 곳입니다."
    }

    fun generatePromptHash(placeName: String, placeDescription: String, category: String, mbti: String): String {
        val prompt = buildMbtiPrompt(placeName, placeDescription, category, mbti)
        return MessageDigest.getInstance("SHA-256")
            .digest(prompt.toByteArray())
            .fold("") { str, it -> str + "%02x".format(it) }
    }
}
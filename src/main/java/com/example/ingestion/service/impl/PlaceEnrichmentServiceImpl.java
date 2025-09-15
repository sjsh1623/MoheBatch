package com.example.ingestion.service.impl;

import com.example.ingestion.service.PlaceEnrichmentService;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Service
public class PlaceEnrichmentServiceImpl implements PlaceEnrichmentService {

    private static final Logger logger = LoggerFactory.getLogger(PlaceEnrichmentServiceImpl.class);

    private final WebClient webClient;
    private final String openaiApiKey;
    private final String openaiModel;
    private final String ollamaBaseUrl;
    private final String ollamaEmbeddingModel;

    public PlaceEnrichmentServiceImpl(
            WebClient webClient,
            @Value("${OPENAI_API_KEY}") String openaiApiKey,
            @Value("${OPENAI_MODEL:gpt-3.5-turbo}") String openaiModel,
            @Value("${OLLAMA_HOST:http://localhost:11434}") String ollamaBaseUrl,
            @Value("${OLLAMA_EMBEDDING_MODEL:mxbai-embed-large}") String ollamaEmbeddingModel
    ) {
        this.webClient = webClient;
        this.openaiApiKey = openaiApiKey;
        this.openaiModel = openaiModel;
        this.ollamaBaseUrl = ollamaBaseUrl;
        this.ollamaEmbeddingModel = ollamaEmbeddingModel;
    }

    @Override
    public Mono<String> generateDescription(String prompt) {
        logger.debug("Generating description using OpenAI GPT API");

        Map<String, Object> requestBody = Map.of(
                "model", openaiModel,
                "messages", List.of(Map.of("role", "user", "content", prompt)),
                "max_tokens", 800,
                "temperature", 0.7,
                "stream", false
        );

        return webClient.post()
                .uri("https://api.openai.com/v1/chat/completions")
                .header("Authorization", "Bearer " + openaiApiKey)
                .header("Content-Type", "application/json")
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(response -> {
                    JsonNode choices = response.get("choices");
                    if (choices != null && choices.isArray() && choices.size() > 0) {
                        JsonNode message = choices.get(0).get("message");
                        if (message != null) {
                            JsonNode content = message.get("content");
                            return content != null ? content.asText() : "";
                        }
                    }
                    return "";
                })
                .doOnNext(result -> logger.debug("Generated description: {} characters", result.length()))
                .doOnError(error -> logger.error("OpenAI description generation failed", error))
                .retryWhen(Retry.backoff(2, Duration.ofSeconds(3))
                        .maxBackoff(Duration.ofSeconds(15))
                        .doBeforeRetry(retrySignal ->
                            logger.warn("Retrying OpenAI description generation, attempt: {}",
                                retrySignal.totalRetries() + 1)))
                .onErrorReturn(""); // Return empty string on failure
    }

    @Override
    public Mono<List<String>> extractKeywords(String text) {
        logger.debug("Extracting keywords using OpenAI GPT API");

        String prompt = String.format("""
                다음 장소 설명에서 핵심 키워드를 추출해주세요.

                설명: %s

                조건:
                1. 5-10개의 핵심 키워드만 추출
                2. 명사 위주로 추출
                3. 쉼표(,)로 구분하여 나열
                4. 예시: 카페, 커피, 디저트, 분위기, 인테리어

                키워드:
                """, text);

        Map<String, Object> requestBody = Map.of(
                "model", openaiModel,
                "messages", List.of(Map.of("role", "user", "content", prompt)),
                "max_tokens", 100,
                "temperature", 0.3,
                "stream", false
        );

        return webClient.post()
                .uri("https://api.openai.com/v1/chat/completions")
                .header("Authorization", "Bearer " + openaiApiKey)
                .header("Content-Type", "application/json")
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(response -> {
                    JsonNode choices = response.get("choices");
                    if (choices != null && choices.isArray() && choices.size() > 0) {
                        JsonNode message = choices.get(0).get("message");
                        if (message != null) {
                            JsonNode content = message.get("content");
                            if (content != null) {
                                String keywordsText = content.asText();
                                return Arrays.stream(keywordsText.split(","))
                                        .map(String::trim)
                                        .filter(s -> !s.isEmpty())
                                        .collect(Collectors.toList());
                            }
                        }
                    }
                    return List.<String>of();
                })
                .doOnNext(keywords -> logger.debug("Extracted {} keywords", keywords.size()))
                .doOnError(error -> logger.error("OpenAI keyword extraction failed", error))
                .retryWhen(Retry.backoff(2, Duration.ofSeconds(3))
                        .maxBackoff(Duration.ofSeconds(15)))
                .onErrorReturn(List.of()); // Return empty list on failure
    }

    @Override
    public Mono<String> generateImagePrompt(String description, String location) {
        logger.debug("Generating image prompt using OpenAI GPT API");

        String prompt = String.format("""
                다음 장소 정보를 바탕으로 이미지 생성용 영어 프롬프트를 작성해주세요.

                장소 설명: %s
                위치: %s

                조건:
                1. 영어로만 작성
                2. 100단어 이내
                3. 사실적이고 매력적인 장면 묘사
                4. "photorealistic, high quality, detailed" 포함
                5. 장소의 분위기와 특징을 잘 표현

                Image Prompt:
                """, description, location);

        Map<String, Object> requestBody = Map.of(
                "model", openaiModel,
                "messages", List.of(Map.of("role", "user", "content", prompt)),
                "max_tokens", 150,
                "temperature", 0.6,
                "stream", false
        );

        return webClient.post()
                .uri("https://api.openai.com/v1/chat/completions")
                .header("Authorization", "Bearer " + openaiApiKey)
                .header("Content-Type", "application/json")
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(response -> {
                    JsonNode choices = response.get("choices");
                    if (choices != null && choices.isArray() && choices.size() > 0) {
                        JsonNode message = choices.get(0).get("message");
                        if (message != null) {
                            JsonNode content = message.get("content");
                            return content != null ? content.asText() : "";
                        }
                    }
                    return "";
                })
                .doOnNext(result -> logger.debug("Generated image prompt: {} characters", result.length()))
                .doOnError(error -> logger.error("OpenAI image prompt generation failed", error))
                .retryWhen(Retry.backoff(2, Duration.ofSeconds(3))
                        .maxBackoff(Duration.ofSeconds(15)))
                .onErrorReturn(""); // Return empty string on failure
    }

    @Override
    public Mono<List<Double>> generateEmbedding(String text) {
        logger.debug("Generating vector embedding using Ollama mxbai-embed-large");

        Map<String, Object> requestBody = Map.of(
                "model", ollamaEmbeddingModel,
                "prompt", text
        );

        return webClient.post()
                .uri(ollamaBaseUrl + "/api/embeddings")
                .header("Content-Type", "application/json")
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(response -> {
                    JsonNode embedding = response.get("embedding");
                    if (embedding != null && embedding.isArray()) {
                        return embedding.findValuesAsText("")
                                .stream()
                                .map(Double::parseDouble)
                                .collect(Collectors.toList());
                    }
                    return List.<Double>of();
                })
                .doOnNext(embedding -> logger.debug("Generated embedding with {} dimensions", embedding.size()))
                .doOnError(error -> logger.error("Ollama embedding generation failed", error))
                .retryWhen(Retry.backoff(2, Duration.ofSeconds(3))
                        .maxBackoff(Duration.ofSeconds(15)))
                .onErrorReturn(List.of()); // Return empty list on failure
    }

    /**
     * Add delay between API calls to prevent rate limiting
     */
    private Mono<Void> addApiDelay() {
        int delayMs = ThreadLocalRandom.current().nextInt(1000, 3000); // 1-3 seconds random delay
        return Mono.delay(Duration.ofMillis(delayMs)).then();
    }
}
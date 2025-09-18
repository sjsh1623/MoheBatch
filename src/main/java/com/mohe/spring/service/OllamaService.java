package com.mohe.spring.service;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohe.spring.config.LlmProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.ResourceAccessException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class OllamaService implements LlmService {
    
    private static final Logger logger = LoggerFactory.getLogger(OllamaService.class);
    
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final LlmProperties llmProperties;
    
    public OllamaService(LlmProperties llmProperties) {
        this.restTemplate = new RestTemplate();
        this.objectMapper = new ObjectMapper();
        this.llmProperties = llmProperties;
    }
    
    /**
     * 텍스트 벡터화 전용 메소드 (mxbai-embed-large 모델 사용)
     */
    public double[] generateEmbedding(String text) {
        try {
            logger.info("Ollama 벡터화 요청: 텍스트 길이 {}", text.length());

            String embeddingResponse = callOllamaEmbedding(text);
            return parseEmbeddingResponse(embeddingResponse);

        } catch (Exception e) {
            logger.error("❌ Ollama 벡터화 실패: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * 키워드 벡터화
     */
    public double[] generateKeywordEmbedding(List<String> keywords) {
        try {
            String keywordText = String.join(" ", keywords);
            return generateEmbedding(keywordText);

        } catch (Exception e) {
            logger.error("❌ Ollama 키워드 벡터화 실패: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Ollama Embedding API 호출
     */
    private String callOllamaEmbedding(String text) {
        try {
            String url = llmProperties.getOllama().getBaseUrl() + "/api/embeddings";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("model", "mxbai-embed-large"); // 사용자 요구사항의 모델
            requestBody.put("prompt", text);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestBody, headers);

            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                return response.getBody();
            } else {
                throw new RuntimeException("Ollama API 호출 실패: " + response.getStatusCode());
            }

        } catch (ResourceAccessException e) {
            logger.error("❌ Ollama 서버 연결 실패. Ollama가 {}에서 실행 중인지 확인하세요",
                        llmProperties.getOllama().getBaseUrl());
            throw new RuntimeException("Ollama 서버 연결 실패", e);
        } catch (Exception e) {
            logger.error("❌ Ollama Embedding API 호출 중 오류", e);
            throw new RuntimeException("Ollama Embedding API 호출 실패", e);
        }
    }

    /**
     * Embedding 응답 파싱
     */
    private double[] parseEmbeddingResponse(String response) {
        try {
            JsonNode jsonNode = objectMapper.readTree(response);
            JsonNode embeddingNode = jsonNode.get("embedding");

            if (embeddingNode != null && embeddingNode.isArray()) {
                double[] embedding = new double[embeddingNode.size()];
                for (int i = 0; i < embeddingNode.size(); i++) {
                    embedding[i] = embeddingNode.get(i).asDouble();
                }
                logger.info("✅ 벡터 생성 완료: 차원 {}", embedding.length);
                return embedding;
            } else {
                throw new RuntimeException("잘못된 embedding 응답 형식");
            }

        } catch (Exception e) {
            logger.error("❌ Embedding 응답 파싱 실패", e);
            throw new RuntimeException("Embedding 응답 파싱 실패", e);
        }
    }

    public OllamaRecommendationResponse generatePlaceRecommendations(String prompt, List<String> availablePlaces) {
        try {
            logger.info("Sending recommendation request to Ollama with prompt length: {}", prompt.length());

            String response = callOllamaGenerate(prompt);
            logger.info("Received response from Ollama, length: {}", response.length());

            return parseRecommendationResponse(response, availablePlaces);

        } catch (Exception e) {
            logger.error("Failed to generate recommendations using Ollama: {}", e.getMessage(), e);
            return createFallbackResponse(availablePlaces);
        }
    }

    /**
     * Generate place recommendations with contextual information
     */
    public OllamaRecommendationResponse generatePlaceRecommendations(
            String userLocation, String weatherCondition, String timeOfDay,
            String userMbti, List<String> availablePlaces) {

        // Build contextual prompt
        String prompt = buildContextualPrompt(userLocation, weatherCondition, timeOfDay, userMbti, availablePlaces);

        // Call the main method
        return generatePlaceRecommendations(prompt, availablePlaces);
    }

    private String buildContextualPrompt(String userLocation, String weatherCondition,
                                       String timeOfDay, String userMbti, List<String> availablePlaces) {
        StringBuilder prompt = new StringBuilder();

        prompt.append("당신은 한국의 장소 추천 전문가입니다.\n\n");
        prompt.append("현재 상황:\n");
        prompt.append("- 위치: ").append(userLocation != null ? userLocation : "서울").append("\n");
        prompt.append("- 날씨: ").append(weatherCondition != null ? weatherCondition : "맑음").append("\n");
        prompt.append("- 시간대: ").append(timeOfDay != null ? timeOfDay : "오후").append("\n");

        if (userMbti != null) {
            prompt.append("- MBTI 성격유형: ").append(userMbti).append("\n");
        }

        prompt.append("\n이용 가능한 장소들:\n");
        for (int i = 0; i < availablePlaces.size() && i < 50; i++) {
            prompt.append("- ").append(availablePlaces.get(i)).append("\n");
        }

        prompt.append("\n현재 상황과 ");
        if (userMbti != null) {
            prompt.append("사용자의 MBTI 성격유형을 고려하여 ");
        }
        prompt.append("가장 적합한 장소 15개를 추천해주세요.\n");
        prompt.append("응답 형식:\n");
        prompt.append("추천장소: [장소1, 장소2, 장소3, ...]\n");
        prompt.append("이유: [추천 이유 설명]");

        return prompt.toString();
    }

    /**
     * Check if Ollama service is available
     */
    public boolean isOllamaAvailable() {
        try {
            String url = llmProperties.getOllama().getBaseUrl() + "/api/tags";
            ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
            return response.getStatusCode() == HttpStatus.OK;
        } catch (Exception e) {
            logger.debug("Ollama service not available: {}", e.getMessage());
            return false;
        }
    }

    private String callOllamaGenerate(String prompt) {
        try {
            String url = llmProperties.getOllama().getBaseUrl() + "/api/generate";
            
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("model", llmProperties.getOllama().getModel());
            requestBody.put("prompt", prompt);
            requestBody.put("stream", false);
            
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);
            
            logger.debug("Calling Ollama API: {}", url);
            ResponseEntity<String> response = restTemplate.exchange(
                url, HttpMethod.POST, request, String.class);
            
            if (response.getStatusCode() == HttpStatus.OK) {
                OllamaApiResponse ollamaResponse = objectMapper.readValue(
                    response.getBody(), OllamaApiResponse.class);
                return ollamaResponse.getResponse();
            } else {
                throw new RuntimeException("Ollama API returned status: " + response.getStatusCode());
            }
            
        } catch (ResourceAccessException e) {
            logger.error("Cannot connect to Ollama server at {}: {}", llmProperties.getOllama().getBaseUrl(), e.getMessage());
            throw new RuntimeException("Ollama server is not accessible", e);
        } catch (Exception e) {
            logger.error("Error calling Ollama API: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to call Ollama API", e);
        }
    }
    
    private OllamaRecommendationResponse parseRecommendationResponse(String response, List<String> availablePlaces) {
        try {
            Pattern pattern = Pattern.compile("추천장소:([^이유]*)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(response);
            
            OllamaRecommendationResponse result = new OllamaRecommendationResponse();
            result.setRawResponse(response);
            
            if (matcher.find()) {
                String placesText = matcher.group(1).trim();
                String[] places = placesText.split("[,，]");
                
                for (String place : places) {
                    String cleanPlace = place.trim().replaceAll("[[\\]\s]", "");
                    if (!cleanPlace.isEmpty()) {
                        for (String availablePlace : availablePlaces) {
                            if (availablePlace.contains(cleanPlace) || cleanPlace.contains(availablePlace)) {
                                result.getRecommendedPlaces().add(availablePlace);
                                break;
                            }
                        }
                    }
                }
            }
            
            Pattern reasonPattern = Pattern.compile("이유:(.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
            Matcher reasonMatcher = reasonPattern.matcher(response);
            if (reasonMatcher.find()) {
                result.setReasoning(reasonMatcher.group(1).trim());
            }
            
            if (result.getRecommendedPlaces().isEmpty()) {
                return createFallbackResponse(availablePlaces);
            }
            
            if (result.getRecommendedPlaces().size() > 15) {
                result.setRecommendedPlaces(result.getRecommendedPlaces().subList(0, 15));
            }
            
            result.setSuccess(true);
            return result;
            
        } catch (Exception e) {
            logger.error("Error parsing Ollama response: {}", e.getMessage(), e);
            return createFallbackResponse(availablePlaces);
        }
    }
    
    private OllamaRecommendationResponse createFallbackResponse(List<String> availablePlaces) {
        OllamaRecommendationResponse response = new OllamaRecommendationResponse();
        response.setSuccess(false);
        response.setRawResponse("Ollama LLM을 사용할 수 없어 기본 추천을 제공합니다.");
        response.setReasoning("현재 시간과 위치를 고려한 기본 추천입니다.");
        
        int limit = Math.min(15, availablePlaces.size());
        response.setRecommendedPlaces(availablePlaces.subList(0, limit));
        
        return response;
    }
    
    
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OllamaApiResponse {
        @JsonProperty("response")
        private String response;
        
        @JsonProperty("done")
        private boolean done;
        
        public String getResponse() { return response; }
        public void setResponse(String response) { this.response = response; }
        public boolean isDone() { return done; }
        public void setDone(boolean done) { this.done = done; }
    }
}

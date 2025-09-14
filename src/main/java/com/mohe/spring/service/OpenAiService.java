package com.mohe.spring.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohe.spring.config.LlmProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class OpenAiService implements LlmService {

    private static final Logger logger = LoggerFactory.getLogger(OpenAiService.class);

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final LlmProperties llmProperties;

    public OpenAiService(LlmProperties llmProperties) {
        this.restTemplate = new RestTemplate();
        this.objectMapper = new ObjectMapper();
        this.llmProperties = llmProperties;
    }

    @Override
    public OllamaRecommendationResponse generatePlaceRecommendations(String prompt, List<String> availablePlaces) {
        try {
            logger.info("Sending recommendation request to OpenAI with prompt length: {}", prompt.length());

            String response = callOpenAi(prompt);
            logger.info("Received response from OpenAI, length: {}", response.length());

            return parseRecommendationResponse(response, availablePlaces);

        } catch (Exception e) {
            logger.error("Failed to generate recommendations using OpenAI: {}", e.getMessage(), e);
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

    private String callOpenAi(String prompt) {
        String url = "https://api.openai.com/v1/chat/completions";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(llmProperties.getOpenai().getApiKey());

        Map<String, Object> message = new HashMap<>();
        message.put("role", "user");
        message.put("content", prompt);

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("model", llmProperties.getOpenai().getModel());
        requestBody.put("messages", Collections.singletonList(message));
        requestBody.put("stream", false);

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);

        ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

        if (response.getStatusCode() == HttpStatus.OK) {
            try {
                OpenAiChatCompletionResponse openAiResponse = objectMapper.readValue(response.getBody(), OpenAiChatCompletionResponse.class);
                return openAiResponse.getChoices().get(0).getMessage().getContent();
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse OpenAI response", e);
            }
        } else {
            throw new RuntimeException("OpenAI API returned status: " + response.getStatusCode());
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
            logger.error("Error parsing OpenAI response: {}", e.getMessage(), e);
            return createFallbackResponse(availablePlaces);
        }
    }

    private OllamaRecommendationResponse createFallbackResponse(List<String> availablePlaces) {
        OllamaRecommendationResponse response = new OllamaRecommendationResponse();
        response.setSuccess(false);
        response.setRawResponse("OpenAI LLM을 사용할 수 없어 기본 추천을 제공합니다.");
        response.setReasoning("현재 시간과 위치를 고려한 기본 추천입니다.");

        int limit = Math.min(15, availablePlaces.size());
        response.setRecommendedPlaces(availablePlaces.subList(0, limit));

        return response;
    }

    private static class OpenAiChatCompletionResponse {
        private List<Choice> choices;

        public List<Choice> getChoices() {
            return choices;
        }

        public void setChoices(List<Choice> choices) {
            this.choices = choices;
        }

        public static class Choice {
            private Message message;

            public Message getMessage() {
                return message;
            }

            public void setMessage(Message message) {
                this.message = message;
            }
        }

        public static class Message {
            private String role;
            private String content;

            public String getRole() {
                return role;
            }

            public void setRole(String role) {
                this.role = role;
            }

            public String getContent() {
                return content;
            }

            public void setContent(String content) {
                this.content = content;
            }
        }
    }
}

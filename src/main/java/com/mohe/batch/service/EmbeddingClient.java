package com.mohe.batch.service;

import com.mohe.batch.dto.embedding.EmbeddingResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.List;

/**
 * Client for communicating with Kanana embedding service
 * Handles HTTP communication with the embedding server
 */
@Service
public class EmbeddingClient {

    private static final Logger log = LoggerFactory.getLogger(EmbeddingClient.class);

    private final RestTemplate restTemplate;
    private final String embeddingServiceUrl;

    public EmbeddingClient(
            @Value("${embedding.service.url:http://localhost:8000}") String embeddingServiceUrl
    ) {
        this.embeddingServiceUrl = embeddingServiceUrl;
        this.restTemplate = createRestTemplate();
        log.info("EmbeddingClient initialized with URL: {}", embeddingServiceUrl);
    }

    private RestTemplate createRestTemplate() {
        RestTemplate template = new RestTemplate();
        // Configure timeout if needed
        return template;
    }

    /**
     * Get embeddings for a list of text strings
     *
     * @param texts List of texts to embed (up to 9 texts recommended)
     * @return EmbeddingResponse containing list of embedding vectors
     * @throws EmbeddingServiceException if the embedding service fails
     */
    public EmbeddingResponse getEmbeddings(List<String> texts) {
        if (texts == null || texts.isEmpty()) {
            log.warn("Attempted to get embeddings for empty text list");
            return new EmbeddingResponse(List.of());
        }

        String url = embeddingServiceUrl + "/embed";
        log.info("Requesting embeddings for {} texts from {}", texts.size(), url);
        log.debug("Texts to embed: {}", texts);

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(List.of(MediaType.APPLICATION_JSON));

            // Send texts directly as JSON array (embedding server expects array, not wrapped object)
            HttpEntity<List<String>> requestEntity = new HttpEntity<>(texts, headers);

            log.debug("Sending request to embedding server...");

            ResponseEntity<EmbeddingResponse> responseEntity = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    requestEntity,
                    EmbeddingResponse.class
            );

            if (responseEntity.getStatusCode() == HttpStatus.OK && responseEntity.getBody() != null) {
                EmbeddingResponse response = responseEntity.getBody();
                log.info("Successfully received {} embeddings", response.getEmbeddingCount());
                return response;
            } else {
                log.error("Unexpected response status: {}", responseEntity.getStatusCode());
                throw new EmbeddingServiceException(
                        "Unexpected response status: " + responseEntity.getStatusCode()
                );
            }

        } catch (HttpClientErrorException e) {
            log.error("Client error when calling embedding service: {} - {}",
                    e.getStatusCode(), e.getResponseBodyAsString());
            throw new EmbeddingServiceException(
                    "Client error: " + e.getStatusCode() + " - " + e.getMessage(), e
            );

        } catch (HttpServerErrorException e) {
            log.error("Server error when calling embedding service: {} - {}",
                    e.getStatusCode(), e.getResponseBodyAsString());
            throw new EmbeddingServiceException(
                    "Server error: " + e.getStatusCode() + " - " + e.getMessage(), e
            );

        } catch (ResourceAccessException e) {
            log.error("Failed to connect to embedding service at {}: {}", url, e.getMessage());
            throw new EmbeddingServiceException(
                    "Failed to connect to embedding service: " + e.getMessage(), e
            );

        } catch (Exception e) {
            log.error("Unexpected error when calling embedding service", e);
            throw new EmbeddingServiceException(
                    "Unexpected error: " + e.getMessage(), e
            );
        }
    }

    /**
     * Get embedding for a single text string
     */
    public float[] getEmbedding(String text) {
        EmbeddingResponse response = getEmbeddings(List.of(text));

        if (!response.hasValidEmbeddings()) {
            throw new EmbeddingServiceException("No embeddings returned for text: " + text);
        }

        List<float[]> embeddings = response.getEmbeddingsAsFloatArrays();
        if (embeddings.isEmpty()) {
            throw new EmbeddingServiceException("Empty embeddings returned for text: " + text);
        }

        return embeddings.get(0);
    }

    /**
     * Check if the embedding service is available
     */
    public boolean isServiceAvailable() {
        try {
            String healthUrl = embeddingServiceUrl + "/health";
            ResponseEntity<String> response = restTemplate.getForEntity(healthUrl, String.class);
            return response.getStatusCode() == HttpStatus.OK;
        } catch (Exception e) {
            log.debug("Embedding service is not available: {}", e.getMessage());
            return false;
        }
    }

    public String getServiceUrl() {
        return embeddingServiceUrl;
    }

    /**
     * Custom exception for embedding service errors
     */
    public static class EmbeddingServiceException extends RuntimeException {
        public EmbeddingServiceException(String message) {
            super(message);
        }

        public EmbeddingServiceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

package com.example.ingestion.service;

import reactor.core.publisher.Mono;
import java.util.List;

/**
 * Place enrichment service interface for handling LLM-based operations
 * Follows strict requirement: OpenAI for text generation, Ollama for embedding only
 */
public interface PlaceEnrichmentService {

    /**
     * Generate place description using OpenAI GPT API
     * @param prompt The prompt for description generation
     * @return Generated description text
     */
    Mono<String> generateDescription(String prompt);

    /**
     * Extract keywords from text using OpenAI GPT API
     * @param text Text to extract keywords from
     * @return List of extracted keywords
     */
    Mono<List<String>> extractKeywords(String text);

    /**
     * Generate image prompt using OpenAI GPT API
     * @param description Place description
     * @param location Location information
     * @return Image generation prompt
     */
    Mono<String> generateImagePrompt(String description, String location);

    /**
     * Generate vector embedding using Ollama mxbai-embed-large model ONLY
     * @param text Text to embed
     * @return Vector embedding
     */
    Mono<List<Double>> generateEmbedding(String text);
}
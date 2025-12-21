package com.mohe.batch.dto.embedding;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response DTO from Kanana embedding service
 * Contains list of embedding vectors
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EmbeddingResponse {

    /**
     * List of embedding vectors
     * Each embedding is a float array of 1792 dimensions
     * Example: [[0.032, -0.019, ...], [0.028, 0.003, ...]]
     */
    @JsonProperty("embeddings")
    private List<List<Double>> embeddings;

    /**
     * Convert embeddings from List<List<Double>> to List<float[]>
     */
    public List<float[]> getEmbeddingsAsFloatArrays() {
        if (embeddings == null) {
            return List.of();
        }

        return embeddings.stream()
            .map(embedding -> {
                float[] floatArray = new float[embedding.size()];
                for (int i = 0; i < embedding.size(); i++) {
                    floatArray[i] = embedding.get(i).floatValue();
                }
                return floatArray;
            })
            .toList();
    }

    /**
     * Check if response has valid embeddings
     */
    public boolean hasValidEmbeddings() {
        return embeddings != null && !embeddings.isEmpty();
    }

    /**
     * Get the number of embeddings in the response
     */
    public int getEmbeddingCount() {
        return embeddings != null ? embeddings.size() : 0;
    }
}

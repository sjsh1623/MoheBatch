package com.mohe.spring.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonType;
import jakarta.persistence.*;
import org.hibernate.annotations.Type;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Entity
@Table(name = "user_preference_vectors")
public class UserPreferenceVector {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id = 0L;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;
    
    @Column(name = "raw_profile_text", nullable = false, columnDefinition = "TEXT")
    private String rawProfileText;
    
    @Column(name = "combined_preferences_text", columnDefinition = "TEXT")
    private String combinedPreferencesText;
    
    // 100-dimensional vector stored as PostgreSQL vector type
    @Column(name = "preference_vector", nullable = false, columnDefinition = "TEXT")
    private String preferenceVector; // Stored as string representation: "[0.1, 0.0, 0.8, ...]"
    
    // Selected 15 keywords with confidences
    @Type(JsonType.class)
    @Column(name = "selected_keywords", nullable = false, columnDefinition = "TEXT")
    private JsonNode selectedKeywords; // [{"keyword_id": 1, "keyword": "specialty_coffee", "confidence": 0.85}, ...]
    
    @Column(name = "extraction_source", nullable = false)
    private String extractionSource = "ollama-openai";
    
    @Column(name = "model_name", nullable = false)
    private String modelName;
    
    @Column(name = "model_version")
    private String modelVersion;
    
    @Column(name = "extraction_prompt_hash")
    private String extractionPromptHash;
    
    @Column(name = "created_at")
    private OffsetDateTime createdAt = OffsetDateTime.now();
    
    @Column(name = "updated_at")
    private OffsetDateTime updatedAt = OffsetDateTime.now();
    
    // Default constructor for JPA
    public UserPreferenceVector() {}
    
    // Constructor with required fields
    public UserPreferenceVector(User user, String rawProfileText, String preferenceVector, 
                               JsonNode selectedKeywords, String modelName) {
        this.user = user;
        this.rawProfileText = rawProfileText;
        this.preferenceVector = preferenceVector;
        this.selectedKeywords = selectedKeywords;
        this.modelName = modelName;
    }
    
    /**
     * Parse the vector string into a float array
     */
    public float[] getVectorAsFloatArray() {
        String vectorStr = preferenceVector.trim();
        if (vectorStr.startsWith("[")) {
            vectorStr = vectorStr.substring(1);
        }
        if (vectorStr.endsWith("]")) {
            vectorStr = vectorStr.substring(0, vectorStr.length() - 1);
        }
        
        String[] parts = vectorStr.split(",");
        float[] result = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Float.parseFloat(parts[i].trim());
        }
        return result;
    }
    
    /**
     * Get selected keywords as a typed list
     */
    public List<SelectedKeyword> getSelectedKeywordsList() {
        List<SelectedKeyword> keywords = new ArrayList<>();
        if (selectedKeywords != null && selectedKeywords.isArray()) {
            for (JsonNode node : selectedKeywords) {
                keywords.add(new SelectedKeyword(
                    node.get("keyword_id").asInt(),
                    node.get("keyword").asText(),
                    node.get("confidence").asDouble()
                ));
            }
        }
        return keywords;
    }
    
    /**
     * Get keyword IDs that have confidence above threshold
     */
    public List<Integer> getHighConfidenceKeywordIds(double threshold) {
        return getSelectedKeywordsList().stream()
                .filter(keyword -> keyword.getConfidence() >= threshold)
                .map(SelectedKeyword::getKeywordId)
                .collect(Collectors.toList());
    }
    
    public List<Integer> getHighConfidenceKeywordIds() {
        return getHighConfidenceKeywordIds(0.5);
    }
    
    /**
     * Calculate Jaccard similarity with another vector based on keyword overlap
     */
    public double calculateJaccardSimilarity(UserPreferenceVector other, double confidenceThreshold) {
        List<Integer> thisKeywords = getSelectedKeywordsList().stream()
                .filter(keyword -> keyword.getConfidence() >= confidenceThreshold)
                .map(SelectedKeyword::getKeywordId)
                .distinct()
                .collect(Collectors.toList());
                
        List<Integer> otherKeywords = other.getSelectedKeywordsList().stream()
                .filter(keyword -> keyword.getConfidence() >= confidenceThreshold)
                .map(SelectedKeyword::getKeywordId)
                .distinct()
                .collect(Collectors.toList());
                
        if (thisKeywords.isEmpty() && otherKeywords.isEmpty()) return 1.0;
        
        List<Integer> intersection = thisKeywords.stream()
                .filter(otherKeywords::contains)
                .collect(Collectors.toList());
                
        List<Integer> union = new ArrayList<>(thisKeywords);
        otherKeywords.stream()
                .filter(keyword -> !union.contains(keyword))
                .forEach(union::add);
        
        return (double) intersection.size() / union.size();
    }
    
    public double calculateJaccardSimilarity(UserPreferenceVector other) {
        return calculateJaccardSimilarity(other, 0.3);
    }
    
    /**
     * Calculate weighted cosine similarity considering confidence scores
     */
    public double calculateWeightedCosineSimilarity(UserPreferenceVector other) {
        float[] thisVector = getVectorAsFloatArray();
        float[] otherVector = other.getVectorAsFloatArray();
        
        if (thisVector.length != otherVector.length) {
            throw new IllegalArgumentException("Vector dimensions must match");
        }
        
        double dotProduct = 0.0;
        double normThis = 0.0;
        double normOther = 0.0;
        
        for (int i = 0; i < thisVector.length; i++) {
            dotProduct += thisVector[i] * otherVector[i];
            normThis += thisVector[i] * thisVector[i];
            normOther += otherVector[i] * otherVector[i];
        }
        
        double normProduct = Math.sqrt(normThis * normOther);
        return normProduct > 0 ? dotProduct / normProduct : 0.0;
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public User getUser() {
        return user;
    }
    
    public void setUser(User user) {
        this.user = user;
    }
    
    public String getRawProfileText() {
        return rawProfileText;
    }
    
    public void setRawProfileText(String rawProfileText) {
        this.rawProfileText = rawProfileText;
    }
    
    public String getCombinedPreferencesText() {
        return combinedPreferencesText;
    }
    
    public void setCombinedPreferencesText(String combinedPreferencesText) {
        this.combinedPreferencesText = combinedPreferencesText;
    }
    
    public String getPreferenceVector() {
        return preferenceVector;
    }
    
    public void setPreferenceVector(String preferenceVector) {
        this.preferenceVector = preferenceVector;
    }
    
    public JsonNode getSelectedKeywords() {
        return selectedKeywords;
    }
    
    public void setSelectedKeywords(JsonNode selectedKeywords) {
        this.selectedKeywords = selectedKeywords;
    }
    
    public String getExtractionSource() {
        return extractionSource;
    }
    
    public void setExtractionSource(String extractionSource) {
        this.extractionSource = extractionSource;
    }
    
    public String getModelName() {
        return modelName;
    }
    
    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
    
    public String getModelVersion() {
        return modelVersion;
    }
    
    public void setModelVersion(String modelVersion) {
        this.modelVersion = modelVersion;
    }
    
    public String getExtractionPromptHash() {
        return extractionPromptHash;
    }
    
    public void setExtractionPromptHash(String extractionPromptHash) {
        this.extractionPromptHash = extractionPromptHash;
    }
    
    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(OffsetDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
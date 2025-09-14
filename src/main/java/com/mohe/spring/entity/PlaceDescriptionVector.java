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
@Table(name = "place_description_vectors")
public class PlaceDescriptionVector {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id = 0L;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", nullable = false)
    private Place place;
    
    @Column(name = "raw_description_text", nullable = false, columnDefinition = "TEXT")
    private String rawDescriptionText;
    
    @Column(name = "combined_attributes_text", columnDefinition = "TEXT")
    private String combinedAttributesText;
    
    // 100-dimensional vector stored as PostgreSQL vector type
    @Column(name = "description_vector", nullable = false, columnDefinition = "TEXT")
    private String descriptionVector; // Stored as string representation: "[0.1, 0.0, 0.8, ...]"
    
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
    public PlaceDescriptionVector() {}
    
    // Constructor with required fields
    public PlaceDescriptionVector(Place place, String rawDescriptionText, String descriptionVector, 
                                 JsonNode selectedKeywords, String modelName) {
        this.place = place;
        this.rawDescriptionText = rawDescriptionText;
        this.descriptionVector = descriptionVector;
        this.selectedKeywords = selectedKeywords;
        this.modelName = modelName;
    }
    
    /**
     * Parse the vector string into a float array
     */
    public float[] getVectorAsFloatArray() {
        String vectorStr = descriptionVector.trim();
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
     * Calculate similarity with user preference vector
     */
    public VectorSimilarityResult calculateSimilarityWithUser(UserPreferenceVector userVector, double mbtiBoost) {
        double cosineScore = calculateWeightedCosineSimilarity(userVector);
        double jaccardScore = calculateJaccardSimilarity(userVector);
        double euclideanDistance = calculateEuclideanDistance(userVector);
        
        // Apply MBTI-based boost if applicable
        double mbtiBoostFactor = calculateMbtiBoost(userVector, mbtiBoost);
        double weightedScore = Math.min((cosineScore * 0.7 + jaccardScore * 0.3) * mbtiBoostFactor, 1.0);
        
        List<Integer> commonKeywords = getCommonKeywordIds(userVector);
        
        return new VectorSimilarityResult(
            cosineScore,
            jaccardScore,
            euclideanDistance,
            mbtiBoostFactor,
            weightedScore,
            commonKeywords.size(),
            getSelectedKeywordsList().size() > 0 ? commonKeywords.size() / 15.0 : 0.0
        );
    }
    
    public VectorSimilarityResult calculateSimilarityWithUser(UserPreferenceVector userVector) {
        return calculateSimilarityWithUser(userVector, 1.2);
    }
    
    /**
     * Calculate Jaccard similarity based on keyword overlap
     */
    private double calculateJaccardSimilarity(UserPreferenceVector userVector, double confidenceThreshold) {
        List<Integer> thisKeywords = getSelectedKeywordsList().stream()
                .filter(keyword -> keyword.getConfidence() >= confidenceThreshold)
                .map(SelectedKeyword::getKeywordId)
                .distinct()
                .collect(Collectors.toList());
                
        List<Integer> userKeywords = userVector.getSelectedKeywordsList().stream()
                .filter(keyword -> keyword.getConfidence() >= confidenceThreshold)
                .map(SelectedKeyword::getKeywordId)
                .distinct()
                .collect(Collectors.toList());
                
        if (thisKeywords.isEmpty() && userKeywords.isEmpty()) return 1.0;
        
        List<Integer> intersection = thisKeywords.stream()
                .filter(userKeywords::contains)
                .collect(Collectors.toList());
                
        List<Integer> union = new ArrayList<>(thisKeywords);
        userKeywords.stream()
                .filter(keyword -> !union.contains(keyword))
                .forEach(union::add);
        
        return (double) intersection.size() / union.size();
    }
    
    private double calculateJaccardSimilarity(UserPreferenceVector userVector) {
        return calculateJaccardSimilarity(userVector, 0.3);
    }
    
    /**
     * Calculate weighted cosine similarity
     */
    private double calculateWeightedCosineSimilarity(UserPreferenceVector userVector) {
        float[] thisVector = getVectorAsFloatArray();
        float[] userVectorArray = userVector.getVectorAsFloatArray();
        
        if (thisVector.length != userVectorArray.length) {
            throw new IllegalArgumentException("Vector dimensions must match");
        }
        
        double dotProduct = 0.0;
        double normThis = 0.0;
        double normUser = 0.0;
        
        for (int i = 0; i < thisVector.length; i++) {
            dotProduct += thisVector[i] * userVectorArray[i];
            normThis += thisVector[i] * thisVector[i];
            normUser += userVectorArray[i] * userVectorArray[i];
        }
        
        double normProduct = Math.sqrt(normThis * normUser);
        return normProduct > 0 ? dotProduct / normProduct : 0.0;
    }
    
    /**
     * Calculate Euclidean distance
     */
    private double calculateEuclideanDistance(UserPreferenceVector userVector) {
        float[] thisVector = getVectorAsFloatArray();
        float[] userVectorArray = userVector.getVectorAsFloatArray();
        
        double sumOfSquares = 0.0;
        for (int i = 0; i < thisVector.length; i++) {
            double diff = thisVector[i] - userVectorArray[i];
            sumOfSquares += diff * diff;
        }
        
        return Math.sqrt(sumOfSquares);
    }
    
    /**
     * Calculate MBTI-based similarity boost
     */
    private double calculateMbtiBoost(UserPreferenceVector userVector, double baseBoost) {
        // Simple implementation - can be enhanced with the keyword catalog rules
        String userMbti = userVector.getUser().getMbti();
        if (userMbti == null) return 1.0;
        
        List<Integer> placeKeywords = getHighConfidenceKeywordIds(0.4);
        
        // Example MBTI-based boosts based on keyword patterns
        if (userMbti.startsWith("I") && placeKeywords.stream().anyMatch(id -> 
            id == 16 || id == 38 || id == 24 || id == 98)) {
            return baseBoost; // Introverts prefer quiet, study-friendly places
        }
        if (userMbti.startsWith("E") && placeKeywords.stream().anyMatch(id -> 
            id == 26 || id == 43 || id == 96 || id == 99)) {
            return baseBoost; // Extraverts prefer social, buzzing places
        }
        if (userMbti.contains("N") && placeKeywords.stream().anyMatch(id -> 
            id == 23 || id == 85 || id == 91 || id == 93)) {
            return baseBoost; // Intuitives prefer creative, unique places
        }
        if (userMbti.contains("S") && placeKeywords.stream().anyMatch(id -> 
            id == 87 || id == 90 || id == 51 || id == 68)) {
            return baseBoost; // Sensors prefer traditional, reliable places
        }
        
        return 1.0;
    }
    
    /**
     * Get common keyword IDs with user vector
     */
    private List<Integer> getCommonKeywordIds(UserPreferenceVector userVector, double confidenceThreshold) {
        List<Integer> thisKeywords = getSelectedKeywordsList().stream()
                .filter(keyword -> keyword.getConfidence() >= confidenceThreshold)
                .map(SelectedKeyword::getKeywordId)
                .distinct()
                .collect(Collectors.toList());
                
        List<Integer> userKeywords = userVector.getSelectedKeywordsList().stream()
                .filter(keyword -> keyword.getConfidence() >= confidenceThreshold)
                .map(SelectedKeyword::getKeywordId)
                .distinct()
                .collect(Collectors.toList());
                
        return thisKeywords.stream()
                .filter(userKeywords::contains)
                .collect(Collectors.toList());
    }
    
    private List<Integer> getCommonKeywordIds(UserPreferenceVector userVector) {
        return getCommonKeywordIds(userVector, 0.3);
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public Place getPlace() {
        return place;
    }
    
    public void setPlace(Place place) {
        this.place = place;
    }
    
    public String getRawDescriptionText() {
        return rawDescriptionText;
    }
    
    public void setRawDescriptionText(String rawDescriptionText) {
        this.rawDescriptionText = rawDescriptionText;
    }
    
    public String getCombinedAttributesText() {
        return combinedAttributesText;
    }
    
    public void setCombinedAttributesText(String combinedAttributesText) {
        this.combinedAttributesText = combinedAttributesText;
    }
    
    public String getDescriptionVector() {
        return descriptionVector;
    }
    
    public void setDescriptionVector(String descriptionVector) {
        this.descriptionVector = descriptionVector;
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


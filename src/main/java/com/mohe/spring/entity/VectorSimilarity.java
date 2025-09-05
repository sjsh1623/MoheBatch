package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.io.Serializable;
import java.util.Objects;

@Entity
@Table(name = "vector_similarities")
@IdClass(VectorSimilarityId.class)
public class VectorSimilarity {
    
    @Id
    @Column(name = "user_id")
    private Long userId;
    
    @Id
    @Column(name = "place_id")
    private Long placeId;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", insertable = false, updatable = false)
    private User user;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", insertable = false, updatable = false)
    private Place place;
    
    @Column(name = "cosine_similarity", precision = 5, scale = 4, nullable = false)
    private BigDecimal cosineSimilarity;
    
    @Column(name = "euclidean_distance", precision = 8, scale = 4, nullable = false)
    private BigDecimal euclideanDistance;
    
    @Column(name = "jaccard_similarity", precision = 5, scale = 4)
    private BigDecimal jaccardSimilarity;
    
    @Column(name = "mbti_boost_factor", precision = 3, scale = 2)
    private BigDecimal mbtiBoostFactor = new BigDecimal("1.0");
    
    @Column(name = "weighted_similarity", precision = 5, scale = 4, nullable = false)
    private BigDecimal weightedSimilarity;
    
    @Column(name = "common_keywords", nullable = false)
    private Integer commonKeywords = 0;
    
    @Column(name = "keyword_overlap_ratio", precision = 3, scale = 2)
    private BigDecimal keywordOverlapRatio;
    
    @Column(name = "calculated_at")
    private OffsetDateTime calculatedAt = OffsetDateTime.now();
    
    @Column(name = "user_vector_version")
    private Long userVectorVersion;
    
    @Column(name = "place_vector_version")
    private Long placeVectorVersion;
    
    // Default constructor for JPA
    public VectorSimilarity() {}
    
    // Constructor with required fields
    public VectorSimilarity(Long userId, Long placeId, BigDecimal cosineSimilarity, 
                           BigDecimal euclideanDistance, BigDecimal weightedSimilarity) {
        this.userId = userId;
        this.placeId = placeId;
        this.cosineSimilarity = cosineSimilarity;
        this.euclideanDistance = euclideanDistance;
        this.weightedSimilarity = weightedSimilarity;
    }
    
    /**
     * Check if this similarity calculation is recent and valid
     */
    public boolean isRecent(long hoursThreshold) {
        return calculatedAt.isAfter(OffsetDateTime.now().minusHours(hoursThreshold));
    }
    
    public boolean isRecent() {
        return isRecent(24);
    }
    
    /**
     * Get similarity score as double
     */
    public double getWeightedSimilarityAsDouble() {
        return weightedSimilarity.doubleValue();
    }
    
    /**
     * Get cosine similarity as double
     */
    public double getCosineSimilarityAsDouble() {
        return cosineSimilarity.doubleValue();
    }
    
    /**
     * Get Jaccard similarity as double
     */
    public double getJaccardSimilarityAsDouble() {
        return jaccardSimilarity != null ? jaccardSimilarity.doubleValue() : 0.0;
    }
    
    /**
     * Check if vectors are compatible (high keyword overlap)
     */
    public boolean hasHighKeywordOverlap(double threshold) {
        return keywordOverlapRatio != null && keywordOverlapRatio.doubleValue() >= threshold;
    }
    
    public boolean hasHighKeywordOverlap() {
        return hasHighKeywordOverlap(0.3);
    }
    
    /**
     * Get MBTI boost factor as double
     */
    public double getMbtiBoostFactorAsDouble() {
        return mbtiBoostFactor.doubleValue();
    }
    
    /**
     * Create VectorSimilarity from calculation result
     */
    public static VectorSimilarity fromCalculationResult(Long userId, Long placeId, VectorSimilarityResult result,
                                                        Long userVectorVersion, Long placeVectorVersion) {
        VectorSimilarity similarity = new VectorSimilarity();
        similarity.setUserId(userId);
        similarity.setPlaceId(placeId);
        similarity.setCosineSimilarity(new BigDecimal(result.getCosineSimilarity()).setScale(4, RoundingMode.HALF_UP));
        similarity.setEuclideanDistance(new BigDecimal(result.getEuclideanDistance()).setScale(4, RoundingMode.HALF_UP));
        similarity.setJaccardSimilarity(new BigDecimal(result.getJaccardSimilarity()).setScale(4, RoundingMode.HALF_UP));
        similarity.setMbtiBoostFactor(new BigDecimal(result.getMbtiBoostFactor()).setScale(2, RoundingMode.HALF_UP));
        similarity.setWeightedSimilarity(new BigDecimal(result.getWeightedSimilarity()).setScale(4, RoundingMode.HALF_UP));
        similarity.setCommonKeywords(result.getCommonKeywords());
        similarity.setKeywordOverlapRatio(new BigDecimal(result.getKeywordOverlapRatio()).setScale(2, RoundingMode.HALF_UP));
        similarity.setUserVectorVersion(userVectorVersion);
        similarity.setPlaceVectorVersion(placeVectorVersion);
        return similarity;
    }
    
    public static VectorSimilarity fromCalculationResult(Long userId, Long placeId, VectorSimilarityResult result) {
        return fromCalculationResult(userId, placeId, result, null, null);
    }
    
    // Getters and Setters
    public Long getUserId() {
        return userId;
    }
    
    public void setUserId(Long userId) {
        this.userId = userId;
    }
    
    public Long getPlaceId() {
        return placeId;
    }
    
    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }
    
    public User getUser() {
        return user;
    }
    
    public void setUser(User user) {
        this.user = user;
    }
    
    public Place getPlace() {
        return place;
    }
    
    public void setPlace(Place place) {
        this.place = place;
    }
    
    public BigDecimal getCosineSimilarity() {
        return cosineSimilarity;
    }
    
    public void setCosineSimilarity(BigDecimal cosineSimilarity) {
        this.cosineSimilarity = cosineSimilarity;
    }
    
    public BigDecimal getEuclideanDistance() {
        return euclideanDistance;
    }
    
    public void setEuclideanDistance(BigDecimal euclideanDistance) {
        this.euclideanDistance = euclideanDistance;
    }
    
    public BigDecimal getJaccardSimilarity() {
        return jaccardSimilarity;
    }
    
    public void setJaccardSimilarity(BigDecimal jaccardSimilarity) {
        this.jaccardSimilarity = jaccardSimilarity;
    }
    
    public BigDecimal getMbtiBoostFactor() {
        return mbtiBoostFactor;
    }
    
    public void setMbtiBoostFactor(BigDecimal mbtiBoostFactor) {
        this.mbtiBoostFactor = mbtiBoostFactor;
    }
    
    public BigDecimal getWeightedSimilarity() {
        return weightedSimilarity;
    }
    
    public void setWeightedSimilarity(BigDecimal weightedSimilarity) {
        this.weightedSimilarity = weightedSimilarity;
    }
    
    public Integer getCommonKeywords() {
        return commonKeywords;
    }
    
    public void setCommonKeywords(Integer commonKeywords) {
        this.commonKeywords = commonKeywords;
    }
    
    public BigDecimal getKeywordOverlapRatio() {
        return keywordOverlapRatio;
    }
    
    public void setKeywordOverlapRatio(BigDecimal keywordOverlapRatio) {
        this.keywordOverlapRatio = keywordOverlapRatio;
    }
    
    public OffsetDateTime getCalculatedAt() {
        return calculatedAt;
    }
    
    public void setCalculatedAt(OffsetDateTime calculatedAt) {
        this.calculatedAt = calculatedAt;
    }
    
    public Long getUserVectorVersion() {
        return userVectorVersion;
    }
    
    public void setUserVectorVersion(Long userVectorVersion) {
        this.userVectorVersion = userVectorVersion;
    }
    
    public Long getPlaceVectorVersion() {
        return placeVectorVersion;
    }
    
    public void setPlaceVectorVersion(Long placeVectorVersion) {
        this.placeVectorVersion = placeVectorVersion;
    }
}
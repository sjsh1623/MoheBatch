package com.mohe.spring.entity;

/**
 * Result of vector similarity calculation
 */
public class VectorSimilarityResult {
    private final double cosineSimilarity;
    private final double jaccardSimilarity;
    private final double euclideanDistance;
    private final double mbtiBoostFactor;
    private final double weightedSimilarity;
    private final int commonKeywords;
    private final double keywordOverlapRatio;
    
    public VectorSimilarityResult(double cosineSimilarity, double jaccardSimilarity, double euclideanDistance,
                                 double mbtiBoostFactor, double weightedSimilarity, int commonKeywords,
                                 double keywordOverlapRatio) {
        this.cosineSimilarity = cosineSimilarity;
        this.jaccardSimilarity = jaccardSimilarity;
        this.euclideanDistance = euclideanDistance;
        this.mbtiBoostFactor = mbtiBoostFactor;
        this.weightedSimilarity = weightedSimilarity;
        this.commonKeywords = commonKeywords;
        this.keywordOverlapRatio = keywordOverlapRatio;
    }
    
    // Getters
    public double getCosineSimilarity() {
        return cosineSimilarity;
    }
    
    public double getJaccardSimilarity() {
        return jaccardSimilarity;
    }
    
    public double getEuclideanDistance() {
        return euclideanDistance;
    }
    
    public double getMbtiBoostFactor() {
        return mbtiBoostFactor;
    }
    
    public double getWeightedSimilarity() {
        return weightedSimilarity;
    }
    
    public int getCommonKeywords() {
        return commonKeywords;
    }
    
    public double getKeywordOverlapRatio() {
        return keywordOverlapRatio;
    }
}
package com.mohe.spring.service

import com.mohe.spring.entity.PlaceSimilarity
import com.mohe.spring.repository.BookmarkRepository
import com.mohe.spring.repository.PlaceRepository
import com.mohe.spring.repository.PlaceSimilarityRepository
import com.mohe.spring.repository.PlaceSimilarityTopKRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.domain.PageRequest
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDateTime
import kotlin.math.exp
import kotlin.math.sqrt

/**
 * Service for calculating MBTI-weighted place similarities using:
 * 1. Jaccard similarity for bookmark overlap
 * 2. Cosine similarity for user behavior vectors
 * 3. MBTI weighting (same MBTI users get higher weight)
 * 4. Time decay for fresher bookmarks
 */
@Service
@Transactional(readOnly = true)
class SimilarityCalculationService(
    private val placeSimilarityRepository: PlaceSimilarityRepository,
    private val placeSimilarityTopKRepository: PlaceSimilarityTopKRepository,
    private val placeRepository: PlaceRepository,
    private val bookmarkRepository: BookmarkRepository,
    @Value("\${mohe.weight.jaccard:0.7}") private val jaccardWeight: Double,
    @Value("\${mohe.weight.cosine:0.3}") private val cosineWeight: Double,
    @Value("\${mohe.weight.sameMbti:2.0}") private val sameMbtiWeight: Double,
    @Value("\${mohe.weight.diffMbti:1.0}") private val diffMbtiWeight: Double,
    @Value("\${mohe.timeDecay.tauDays:30}") private val timeDecayTauDays: Long,
    @Value("\${mohe.recommendation.topK:100}") private val topK: Int
) {

    private val logger = LoggerFactory.getLogger(SimilarityCalculationService::class.java)

    /**
     * Calculate and store similarity between all place pairs based on bookmark co-occurrence
     * with MBTI weighting and time decay
     */
    @Async("similarityExecutor")
    @Transactional
    fun calculateAllPlaceSimilarities() {
        logger.info("Starting similarity calculation for all places with MBTI weighting")
        
        val startTime = System.currentTimeMillis()
        val timeDecayStart = LocalDateTime.now().minusDays(timeDecayTauDays * 3) // 3 tau for effective cutoff
        
        try {
            // Get co-occurrence data with MBTI weighting
            val coOccurrences = placeSimilarityRepository.findBookmarkCoOccurrences(
                sameMbtiWeight = sameMbtiWeight,
                diffMbtiWeight = diffMbtiWeight,
                timeDecayStart = timeDecayStart
            )
            
            logger.info("Found ${coOccurrences.size} place co-occurrences to process")
            
            val placeUserCounts = mutableMapOf<Long, Int>()
            val placePairs = mutableListOf<Pair<Long, Long>>()
            
            coOccurrences.forEach { row ->
                val placeId1 = (row["place_id"] as Number).toLong()
                val placeId2 = (row["other_place_id"] as Number).toLong()
                val commonBookmarks = (row["common_bookmarks"] as Number).toInt()
                val weightedCo = (row["weighted_co"] as Number).toDouble()
                
                // Track unique user counts per place (for Jaccard denominator)
                placeUserCounts[placeId1] = placeUserCounts.getOrDefault(placeId1, 0) + 1
                placeUserCounts[placeId2] = placeUserCounts.getOrDefault(placeId2, 0) + 1
                
                placePairs.add(Pair(placeId1, placeId2))
            }
            
            // Calculate similarities in batches
            val batchSize = 1000
            var processedCount = 0
            
            coOccurrences.chunked(batchSize).forEach { batch ->
                calculateSimilarityBatch(batch, placeUserCounts, timeDecayStart)
                processedCount += batch.size
                
                if (processedCount % 5000 == 0) {
                    logger.info("Processed $processedCount / ${coOccurrences.size} similarities")
                }
            }
            
            val elapsedTime = System.currentTimeMillis() - startTime
            logger.info("Completed similarity calculation: ${coOccurrences.size} pairs processed in ${elapsedTime}ms")
            
        } catch (ex: Exception) {
            logger.error("Error calculating place similarities", ex)
            throw ex
        }
    }
    
    @Transactional
    private fun calculateSimilarityBatch(
        batch: List<Map<String, Any>>, 
        placeUserCounts: Map<Long, Int>,
        timeDecayStart: LocalDateTime
    ) {
        batch.forEach { row ->
            try {
                val placeId1 = (row["place_id"] as Number).toLong()
                val placeId2 = (row["other_place_id"] as Number).toLong()
                val commonBookmarks = (row["common_bookmarks"] as Number).toInt()
                val weightedCo = (row["weighted_co"] as Number).toDouble()
                
                // Calculate Jaccard similarity with time decay
                val users1 = placeUserCounts[placeId1] ?: 0
                val users2 = placeUserCounts[placeId2] ?: 0
                val unionSize = users1 + users2 - commonBookmarks
                
                val jaccard = if (unionSize > 0) {
                    calculateTimeDecayedSimilarity(commonBookmarks.toDouble(), unionSize.toDouble(), timeDecayStart)
                } else 0.0
                
                // Calculate Cosine similarity with MBTI weighting
                val cosine = if (users1 > 0 && users2 > 0) {
                    calculateMbtiWeightedCosine(weightedCo, sqrt(users1.toDouble() * users2.toDouble()))
                } else 0.0
                
                // Store similarity (ensuring consistent ordering: smaller ID first)
                val orderedPlaceId1 = minOf(placeId1, placeId2)
                val orderedPlaceId2 = maxOf(placeId1, placeId2)
                
                placeSimilarityRepository.upsertSimilarity(
                    placeId1 = orderedPlaceId1,
                    placeId2 = orderedPlaceId2,
                    jaccard = BigDecimal(jaccard).setScale(4, RoundingMode.HALF_UP),
                    cosineBin = BigDecimal(cosine).setScale(4, RoundingMode.HALF_UP),
                    coUsers = commonBookmarks,
                    updatedAt = LocalDateTime.now()
                )
                
            } catch (ex: Exception) {
                logger.warn("Failed to process similarity for row: $row", ex)
            }
        }
    }
    
    /**
     * Calculate time-decayed similarity score
     * Uses exponential decay: score * exp(-time_diff / tau)
     */
    private fun calculateTimeDecayedSimilarity(intersection: Double, union: Double, timeDecayStart: LocalDateTime): Double {
        val baseScore = intersection / union
        
        // Apply time decay (bookmarks older than timeDecayStart get less weight)
        val daysSinceDecayStart = java.time.temporal.ChronoUnit.DAYS.between(timeDecayStart, LocalDateTime.now())
        val decayFactor = exp(-daysSinceDecayStart.toDouble() / timeDecayTauDays)
        
        return baseScore * decayFactor
    }
    
    /**
     * Calculate MBTI-weighted cosine similarity
     * Higher weight for same-MBTI user interactions
     */
    private fun calculateMbtiWeightedCosine(weightedSum: Double, normalizationFactor: Double): Double {
        return if (normalizationFactor > 0) {
            (weightedSum / normalizationFactor).coerceIn(0.0, 1.0)
        } else 0.0
    }
    
    /**
     * Refresh Top-K cache for a specific place
     * Called when place bookmarks change significantly
     */
    @Async("similarityExecutor")
    @Transactional
    fun refreshTopKSimilarities(placeId: Long) {
        logger.debug("Refreshing Top-K similarities for place ID: $placeId")
        
        try {
            // Clear existing Top-K for this place
            placeSimilarityTopKRepository.deleteByPlaceId(placeId)
            
            // Rebuild Top-K using weighted combination of jaccard and cosine
            placeSimilarityTopKRepository.refreshTopKForPlace(
                placeId = placeId,
                topK = topK,
                jaccardWeight = jaccardWeight,
                cosineWeight = cosineWeight,
                updatedAt = LocalDateTime.now()
            )
            
            logger.debug("Successfully refreshed Top-K similarities for place ID: $placeId")
            
        } catch (ex: Exception) {
            logger.error("Failed to refresh Top-K similarities for place ID: $placeId", ex)
            throw ex
        }
    }
    
    /**
     * Refresh Top-K cache for multiple places
     * Useful for batch updates
     */
    @Async("similarityExecutor")
    @Transactional
    fun refreshTopKSimilarities(placeIds: List<Long>) {
        logger.info("Refreshing Top-K similarities for ${placeIds.size} places")
        
        placeIds.forEach { placeId ->
            try {
                refreshTopKSimilarities(placeId)
            } catch (ex: Exception) {
                logger.warn("Failed to refresh Top-K for place ID: $placeId", ex)
            }
        }
        
        logger.info("Completed Top-K refresh for ${placeIds.size} places")
    }
    
    /**
     * Calculate similarity for a specific place pair
     * Used for real-time updates when new bookmarks are created
     */
    @Transactional
    fun calculatePlacePairSimilarity(placeId1: Long, placeId2: Long) {
        logger.debug("Calculating similarity between places: $placeId1 and $placeId2")
        
        val timeDecayStart = LocalDateTime.now().minusDays(timeDecayTauDays * 3)
        
        try {
            // Get co-occurrence data for this specific pair
            val coOccurrences = placeSimilarityRepository.findBookmarkCoOccurrences(
                sameMbtiWeight = sameMbtiWeight,
                diffMbtiWeight = diffMbtiWeight,
                timeDecayStart = timeDecayStart
            ).filter { row ->
                val pid1 = (row["place_id"] as Number).toLong()
                val pid2 = (row["other_place_id"] as Number).toLong()
                (pid1 == placeId1 && pid2 == placeId2) || (pid1 == placeId2 && pid2 == placeId1)
            }
            
            if (coOccurrences.isNotEmpty()) {
                val placeUserCounts = mutableMapOf<Long, Int>()
                
                // Calculate user counts for denominator
                coOccurrences.forEach { row ->
                    val pid1 = (row["place_id"] as Number).toLong()
                    val pid2 = (row["other_place_id"] as Number).toLong()
                    placeUserCounts[pid1] = placeUserCounts.getOrDefault(pid1, 0) + 1
                    placeUserCounts[pid2] = placeUserCounts.getOrDefault(pid2, 0) + 1
                }
                
                calculateSimilarityBatch(coOccurrences, placeUserCounts, timeDecayStart)
                
                // Update Top-K cache for both places
                refreshTopKSimilarities(placeId1)
                refreshTopKSimilarities(placeId2)
            }
            
        } catch (ex: Exception) {
            logger.error("Failed to calculate similarity for places: $placeId1 and $placeId2", ex)
            throw ex
        }
    }
    
    /**
     * Get similarity statistics for monitoring
     */
    fun getSimilarityStatistics(): SimilarityStatistics {
        return try {
            val totalSimilarities = placeSimilarityRepository.count()
            val totalTopKEntries = placeSimilarityTopKRepository.count()
            
            // Get some sample similarities for quality check
            val recentSimilarities = placeSimilarityRepository.findAll(PageRequest.of(0, 100))
                .content
            
            val avgJaccard = recentSimilarities.map { it.jaccard.toDouble() }.average()
            val avgCosine = recentSimilarities.map { it.cosineBin.toDouble() }.average()
            
            SimilarityStatistics(
                totalSimilarities = totalSimilarities,
                totalTopKEntries = totalTopKEntries,
                averageJaccard = avgJaccard,
                averageCosine = avgCosine,
                sampleSize = recentSimilarities.size
            )
            
        } catch (ex: Exception) {
            logger.error("Failed to get similarity statistics", ex)
            SimilarityStatistics(0, 0, 0.0, 0.0, 0)
        }
    }
}

/**
 * Statistics about similarity calculations
 */
data class SimilarityStatistics(
    val totalSimilarities: Long,
    val totalTopKEntries: Long,
    val averageJaccard: Double,
    val averageCosine: Double,
    val sampleSize: Int
)
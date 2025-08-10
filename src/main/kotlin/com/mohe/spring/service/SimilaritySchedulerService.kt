package com.mohe.spring.service

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Scheduler service for automated similarity calculations
 * Runs periodically to keep similarity matrices up-to-date
 */
@Service
@ConditionalOnProperty(
    value = ["mohe.similarity.scheduling.enabled"], 
    havingValue = "true", 
    matchIfMissing = true
)
class SimilaritySchedulerService(
    private val similarityCalculationService: SimilarityCalculationService
) {

    private val logger = LoggerFactory.getLogger(SimilaritySchedulerService::class.java)
    private val isRunning = AtomicBoolean(false)

    /**
     * Run full similarity calculation every 4 hours
     * Only runs if previous calculation is complete
     */
    @Scheduled(cron = "\${mohe.similarity.scheduling.cron:0 0 */4 * * ?}") // Every 4 hours
    @Async("similarityExecutor")
    fun scheduledSimilarityCalculation() {
        if (!isRunning.compareAndSet(false, true)) {
            logger.info("Similarity calculation already in progress, skipping scheduled run")
            return
        }

        try {
            logger.info("Starting scheduled similarity calculation")
            val startTime = System.currentTimeMillis()

            similarityCalculationService.calculateAllPlaceSimilarities()

            val elapsedTime = System.currentTimeMillis() - startTime
            logger.info("Completed scheduled similarity calculation in ${elapsedTime}ms")

        } catch (ex: Exception) {
            logger.error("Scheduled similarity calculation failed", ex)
        } finally {
            isRunning.set(false)
        }
    }

    /**
     * Refresh Top-K cache more frequently for better responsiveness
     * This is lighter than full similarity calculation
     */
    @Scheduled(cron = "\${mohe.similarity.topk.refresh.cron:0 */30 * * * ?}") // Every 30 minutes
    @Async("taskExecutor")
    fun scheduledTopKRefresh() {
        try {
            logger.debug("Starting scheduled Top-K refresh for active places")

            // Get places that have received bookmarks in the last day
            // This could be enhanced to track "dirty" places that need refresh
            val activePlaceIds = getActivePlaceIds()
            
            if (activePlaceIds.isNotEmpty()) {
                logger.info("Refreshing Top-K for ${activePlaceIds.size} active places")
                similarityCalculationService.refreshTopKSimilarities(activePlaceIds)
            }

        } catch (ex: Exception) {
            logger.error("Scheduled Top-K refresh failed", ex)
        }
    }

    /**
     * Get list of places that have received bookmarks recently
     * These are candidates for Top-K refresh
     */
    private fun getActivePlaceIds(): List<Long> {
        // For now, return empty list
        // This could be enhanced to query places with recent bookmark activity
        // or maintain a "dirty places" cache
        return emptyList()
    }

    /**
     * Check if similarity calculation is currently running
     */
    fun isCalculationRunning(): Boolean {
        return isRunning.get()
    }

    /**
     * Manual trigger for similarity calculation
     * Returns immediately, calculation runs async
     */
    fun triggerSimilarityCalculation(): Boolean {
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("Cannot trigger similarity calculation - already running")
            return false
        }

        try {
            logger.info("Manually triggered similarity calculation")
            similarityCalculationService.calculateAllPlaceSimilarities()
            return true
        } catch (ex: Exception) {
            logger.error("Manual similarity calculation trigger failed", ex)
            isRunning.set(false)
            throw ex
        }
    }
}
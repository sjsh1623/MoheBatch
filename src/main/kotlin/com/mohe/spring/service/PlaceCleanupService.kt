package com.mohe.spring.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.mohe.spring.entity.Place
import com.mohe.spring.repository.PlaceRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.domain.PageRequest
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.util.retry.Retry
import java.math.BigDecimal
import java.time.Duration
import java.time.LocalDate
import java.time.OffsetDateTime

/**
 * Service for maintaining place quality by cleaning up old low-rated places
 * and rechecking ratings for places older than 6 months
 */
@Service
class PlaceCleanupService(
    private val placeRepository: PlaceRepository,
    private val webClient: WebClient,
    private val objectMapper: ObjectMapper,
    @Value("\${google.places.apiKey}") private val googleApiKey: String
) {

    private val logger = LoggerFactory.getLogger(PlaceCleanupService::class.java)

    /**
     * Scheduled cleanup job to remove old low-rated places
     * Runs daily at 2 AM to minimize impact on users
     */
    @Scheduled(cron = "\${mohe.cleanup.cron:0 0 2 * * ?}") // Daily at 2 AM
    @Async("taskExecutor")
    @Transactional
    fun scheduledPlaceCleanup() {
        logger.info("Starting scheduled place cleanup")
        
        try {
            val recheckThreshold = OffsetDateTime.now().minusHours(6) // Don't recheck too frequently
            val batchSize = 20 // Process in small batches to avoid API rate limits
            
            var processedCount = 0
            var removedCount = 0
            var updatedCount = 0
            
            do {
                val placesToRecheck = placeRepository.findPlacesNeedingRatingRecheck(
                    recheckThreshold, 
                    PageRequest.of(0, batchSize)
                )
                
                if (placesToRecheck.isEmpty) break
                
                for (place in placesToRecheck.content) {
                    try {
                        val action = recheckPlaceRating(place)
                        processedCount++
                        
                        when (action) {
                            CleanupAction.REMOVED -> removedCount++
                            CleanupAction.UPDATED -> updatedCount++
                            CleanupAction.KEPT -> { /* no action needed */ }
                        }
                        
                        // Rate limiting for Google API
                        Thread.sleep(100)
                        
                    } catch (ex: Exception) {
                        logger.warn("Failed to recheck place ${place.id}: ${ex.message}")
                    }
                }
                
            } while (placesToRecheck.hasContent() && processedCount < 100) // Limit total per run
            
            logger.info("Cleanup completed: processed=$processedCount, removed=$removedCount, updated=$updatedCount")
            
        } catch (ex: Exception) {
            logger.error("Failed to complete scheduled place cleanup", ex)
        }
    }

    /**
     * Recheck a specific place's rating and decide whether to keep or remove it
     */
    @Transactional
    fun recheckPlaceRating(place: Place): CleanupAction {
        logger.debug("Rechecking rating for place ${place.id}: ${place.name}")
        
        try {
            // Get current rating from Google Places API
            val currentRating = fetchCurrentRatingFromGoogle(place)
            
            val now = OffsetDateTime.now()
            val isRecentlyOpened = place.isRecentlyOpened()
            val hasGoodRating = (currentRating ?: 0.0) >= 3.0
            
            return when {
                // Keep if recently opened regardless of rating
                isRecentlyOpened -> {
                    updatePlaceRatingData(place, currentRating, now, false)
                    CleanupAction.KEPT
                }
                
                // Keep if good rating
                hasGoodRating -> {
                    updatePlaceRatingData(place, currentRating, now, false)
                    CleanupAction.UPDATED
                }
                
                // Remove if old and low rating
                else -> {
                    logger.info("Removing place ${place.id} (${place.name}) - rating: $currentRating, age: ${place.isOlderThanSixMonths()}")
                    placeRepository.delete(place)
                    CleanupAction.REMOVED
                }
            }
            
        } catch (ex: Exception) {
            logger.warn("Failed to recheck place ${place.id}, marking for later retry", ex)
            
            // Mark for retry later but don't delete
            updatePlaceRatingData(place, null, OffsetDateTime.now().minusHours(1), true)
            return CleanupAction.KEPT
        }
    }

    /**
     * Fetch current rating from Google Places API
     */
    private fun fetchCurrentRatingFromGoogle(place: Place): Double? {
        val googlePlaceId = place.googlePlaceId
        
        if (googlePlaceId == null) {
            logger.debug("No Google Place ID for place ${place.id}, skipping rating check")
            return null
        }
        
        try {
            val detailsUrl = "https://maps.googleapis.com/maps/api/place/details/json" +
                    "?place_id=$googlePlaceId" +
                    "&fields=rating,user_ratings_total" +
                    "&key=$googleApiKey"

            val response = webClient.get()
                .uri(detailsUrl)
                .retrieve()
                .bodyToMono(String::class.java)
                .retryWhen(
                    Retry.backoff(2, Duration.ofSeconds(1))
                        .filter { it !is WebClientResponseException || it.statusCode.is5xxServerError }
                )
                .timeout(Duration.ofSeconds(10))
                .block()

            return parseGoogleRatingResponse(response ?: "")
            
        } catch (ex: Exception) {
            logger.warn("Failed to fetch rating from Google for place ${place.id}", ex)
            throw ex
        }
    }

    /**
     * Parse Google Places Details API response to extract rating
     */
    private fun parseGoogleRatingResponse(response: String): Double? {
        try {
            val jsonNode = objectMapper.readTree(response)
            val status = jsonNode.get("status")?.asText()
            
            if (status != "OK") {
                logger.warn("Google API returned status: $status")
                return null
            }
            
            val result = jsonNode.get("result")
            return result?.get("rating")?.asDouble()
            
        } catch (ex: Exception) {
            logger.warn("Failed to parse Google rating response", ex)
            return null
        }
    }

    /**
     * Update place with new rating data and cleanup flags
     */
    private fun updatePlaceRatingData(
        place: Place, 
        newRating: Double?,
        lastCheck: OffsetDateTime,
        shouldRetry: Boolean
    ) {
        try {
            val updatedPlace = place.copy(
                rating = newRating?.let { BigDecimal(it) } ?: place.rating,
                lastRatingCheck = lastCheck,
                shouldRecheckRating = shouldRetry,
                updatedAt = lastCheck.toLocalDateTime()
            )
            
            placeRepository.save(updatedPlace)
            
        } catch (ex: Exception) {
            logger.warn("Failed to update place rating data for place ${place.id}", ex)
        }
    }

    /**
     * Manual cleanup trigger for admin use
     */
    @Transactional
    fun triggerManualCleanup(maxPlacesToCheck: Int = 50): CleanupStats {
        logger.info("Starting manual place cleanup for up to $maxPlacesToCheck places")
        
        val stats = CleanupStats()
        val recheckThreshold = OffsetDateTime.now().minusMinutes(10) // More recent for manual trigger
        
        try {
            val placesToRecheck = placeRepository.findPlacesNeedingRatingRecheck(
                recheckThreshold,
                PageRequest.of(0, maxPlacesToCheck)
            )
            
            placesToRecheck.content.forEach { place ->
                try {
                    val action = recheckPlaceRating(place)
                    stats.processed++
                    
                    when (action) {
                        CleanupAction.REMOVED -> stats.removed++
                        CleanupAction.UPDATED -> stats.updated++
                        CleanupAction.KEPT -> stats.kept++
                    }
                    
                } catch (ex: Exception) {
                    stats.errors++
                    logger.warn("Failed to recheck place ${place.id} during manual cleanup", ex)
                }
            }
            
            logger.info("Manual cleanup completed: $stats")
            return stats
            
        } catch (ex: Exception) {
            logger.error("Failed to complete manual cleanup", ex)
            stats.errors++
            return stats
        }
    }

    /**
     * Get statistics about places that need cleanup
     */
    fun getCleanupStatistics(): CleanupStatistics {
        return try {
            val total = placeRepository.count()
            val needingRecheck = placeRepository.findPlacesNeedingRatingRecheck(
                OffsetDateTime.now().minusHours(24),
                PageRequest.of(0, 1000)
            ).totalElements
            
            val newPlaces = placeRepository.countRecommendablePlaces()
            val oldPlaces = total - newPlaces
            
            CleanupStatistics(
                totalPlaces = total,
                placesNeedingRecheck = needingRecheck,
                recommendablePlaces = newPlaces,
                potentialForCleanup = oldPlaces
            )
            
        } catch (ex: Exception) {
            logger.error("Failed to get cleanup statistics", ex)
            CleanupStatistics(0, 0, 0, 0)
        }
    }
}

// Enums and data classes for cleanup operations
enum class CleanupAction {
    REMOVED, UPDATED, KEPT
}

data class CleanupStats(
    var processed: Int = 0,
    var removed: Int = 0,
    var updated: Int = 0,
    var kept: Int = 0,
    var errors: Int = 0
) {
    override fun toString(): String {
        return "processed=$processed, removed=$removed, updated=$updated, kept=$kept, errors=$errors"
    }
}

data class CleanupStatistics(
    val totalPlaces: Long,
    val placesNeedingRecheck: Long,
    val recommendablePlaces: Long,
    val potentialForCleanup: Long
)
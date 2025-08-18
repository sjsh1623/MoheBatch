package com.mohe.spring.service

import com.mohe.spring.dto.*
import com.mohe.spring.entity.Place
import com.mohe.spring.entity.User
import com.mohe.spring.entity.PlaceSimilarityTopK
import com.mohe.spring.repository.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDateTime
import kotlin.math.exp
import kotlin.math.ln

/**
 * Enhanced recommendation service that uses MBTI-weighted similarity calculations
 * to provide personalized place recommendations
 */
@Service
@Transactional(readOnly = true)
class EnhancedRecommendationService(
    private val placeRepository: PlaceRepository,
    private val bookmarkRepository: BookmarkRepository,
    private val placeSimilarityTopKRepository: PlaceSimilarityTopKRepository,
    private val placeMbtiDescriptionRepository: PlaceMbtiDescriptionRepository,
    private val dynamicPlaceFetchingService: DynamicPlaceFetchingService,
    @Value("\${mohe.recommendation.topK:100}") private val topK: Int,
    @Value("\${mohe.weight.jaccard:0.7}") private val jaccardWeight: Double,
    @Value("\${mohe.weight.cosine:0.3}") private val cosineWeight: Double,
    @Value("\${mohe.weight.mbtiBoost:0.3}") private val mbtiBoost: Double,
    @Value("\${mohe.weight.popularityPenalty:0.1}") private val popularityPenalty: Double,
    @Value("\${mohe.diversity.enabled:true}") private val diversityEnabled: Boolean,
    @Value("\${mohe.recommendation.minPlacesThreshold:50}") private val minPlacesThreshold: Int
) {

    private val logger = LoggerFactory.getLogger(EnhancedRecommendationService::class.java)

    /**
     * Generate enhanced recommendations for a user based on:
     * 1. User's bookmark history
     * 2. MBTI-weighted place similarities
     * 3. Diversity and popularity balancing
     * 4. MBTI-specific place descriptions
     * 5. Dynamic place fetching when insufficient data
     */
    fun getEnhancedRecommendations(
        user: User,
        limit: Int = 15,
        excludeBookmarked: Boolean = true,
        category: String? = null
    ): EnhancedRecommendationsResponse {
        logger.debug("Generating enhanced recommendations for user ${user.id} with MBTI: ${user.mbti}")
        
        try {
            // Check if we have sufficient places for recommendations
            val availablePlaceCount = dynamicPlaceFetchingService.checkAndFetchPlacesIfNeeded(
                minRequiredPlaces = minPlacesThreshold,
                category = category
            )
            
            logger.debug("Available recommendable places: $availablePlaceCount")
            
            // Get user's bookmarked places
            val userBookmarks = bookmarkRepository.findByUserOrderByCreatedAtDesc(user)
                .map { it.place.id!! }
            
            if (userBookmarks.isEmpty()) {
                logger.debug("User has no bookmarks, using rating-based recommendations with quality filtering")
                return getRatingBasedRecommendations(user, limit, category)
            }

            // Get similarity-based recommendations
            val candidateRecommendations = generateSimilarityBasedRecommendations(
                userBookmarks, 
                user.mbti,
                limit * 3, // Get more candidates for diversity filtering
                category
            )
            
            // Apply diversity and re-ranking
            val diversifiedRecommendations = if (diversityEnabled) {
                applyDiversityFiltering(candidateRecommendations, limit)
            } else {
                candidateRecommendations.take(limit)
            }
            
            // Filter out bookmarked places if requested
            val filteredRecommendations = if (excludeBookmarked) {
                diversifiedRecommendations.filter { it.placeId !in userBookmarks }
            } else {
                diversifiedRecommendations
            }.take(limit)
            
            // Convert to response format with MBTI-specific descriptions
            val recommendations = buildRecommendationResponseList(filteredRecommendations, user)
            
            logger.debug("Generated ${recommendations.size} enhanced recommendations for user ${user.id}")
            
            return EnhancedRecommendationsResponse(
                recommendations = recommendations,
                algorithm = "mbti_similarity_based",
                totalCount = recommendations.size,
                userMbti = user.mbti,
                basedOnBookmarks = userBookmarks.size
            )
            
        } catch (ex: Exception) {
            logger.error("Failed to generate enhanced recommendations for user ${user.id}", ex)
            return getRatingBasedRecommendations(user, limit, category)
        }
    }
    
    /**
     * Generate candidate recommendations based on similarity to user's bookmarked places
     */
    private fun generateSimilarityBasedRecommendations(
        userBookmarks: List<Long>,
        userMbti: String?,
        candidateLimit: Int,
        category: String? = null
    ): List<RecommendationCandidate> {
        val candidateScores = mutableMapOf<Long, Double>()
        val candidateReasons = mutableMapOf<Long, MutableList<String>>()
        
        userBookmarks.forEach { bookmarkedPlaceId ->
            // Get top similar places for each bookmarked place
            val similarPlaces = placeSimilarityTopKRepository.findByPlaceIdOrderByRank(bookmarkedPlaceId)
                .take(topK / userBookmarks.size.coerceAtLeast(1)) // Distribute topK among bookmarks
            
            similarPlaces.forEach { similarity ->
                val candidateId = similarity.neighborPlaceId
                val baseScore = calculateCombinedSimilarity(similarity)
                
                // Apply MBTI boost if available
                val mbtiAdjustedScore = if (userMbti != null) {
                    applyMbtiBoost(baseScore, userMbti, candidateId)
                } else {
                    baseScore
                }
                
                // Apply time decay for fresher recommendations
                val timeAdjustedScore = applyTimeDecay(mbtiAdjustedScore, similarity.updatedAt)
                
                // Accumulate scores (places similar to multiple bookmarks get higher scores)
                candidateScores[candidateId] = candidateScores.getOrDefault(candidateId, 0.0) + timeAdjustedScore
                
                // Track recommendation reasons
                candidateReasons.getOrPut(candidateId) { mutableListOf() }
                    .add("Similar to your bookmarked place")
            }
        }
        
        // Apply popularity penalty to avoid always recommending popular places
        val adjustedCandidates = candidateScores.map { (placeId, score) ->
            val place = placeRepository.findById(placeId).orElse(null)
            val adjustedScore = if (place != null) {
                applyPopularityPenalty(score, place)
            } else score
            
            RecommendationCandidate(
                placeId = placeId,
                score = adjustedScore,
                reasons = candidateReasons[placeId] ?: mutableListOf(),
                place = place
            )
        }.filter { candidate ->
            val place = candidate.place
            place != null && place.shouldBeRecommended() && 
            (category == null || place.category == category)
        }.sortedByDescending { it.score }
            .take(candidateLimit)
        
        return adjustedCandidates
    }
    
    /**
     * Calculate combined similarity score from jaccard and cosine similarity
     */
    private fun calculateCombinedSimilarity(similarity: PlaceSimilarityTopK): Double {
        val jaccardScore = similarity.jaccard.toDouble()
        val cosineScore = similarity.cosineBin.toDouble()
        return jaccardWeight * jaccardScore + cosineWeight * cosineScore
    }
    
    /**
     * Apply MBTI boost based on place's suitability for the user's MBTI type
     */
    private fun applyMbtiBoost(baseScore: Double, userMbti: String, placeId: Long): Double {
        try {
            val mbtiDescription = placeMbtiDescriptionRepository.findByPlaceIdAndMbti(placeId, userMbti)
            return if (mbtiDescription != null) {
                // Boost score for places with MBTI-specific descriptions
                baseScore * (1.0 + mbtiBoost)
            } else {
                baseScore
            }
        } catch (ex: Exception) {
            logger.warn("Failed to apply MBTI boost for place $placeId and MBTI $userMbti", ex)
            return baseScore
        }
    }
    
    /**
     * Apply time decay to prioritize places with recent similarity calculations
     */
    private fun applyTimeDecay(score: Double, lastUpdated: LocalDateTime): Double {
        val daysSinceUpdate = java.time.temporal.ChronoUnit.DAYS.between(lastUpdated, LocalDateTime.now())
        val decayFactor = exp(-daysSinceUpdate.toDouble() / 7.0) // 7-day half-life
        return score * (0.5 + 0.5 * decayFactor) // Minimum 50% of original score
    }
    
    /**
     * Apply popularity penalty to reduce bias toward highly popular places
     */
    private fun applyPopularityPenalty(score: Double, place: Place): Double {
        val popularityNorm = place.reviewCount.toDouble() / 1000.0 // Normalize by 1000 reviews
        val penalty = ln(1.0 + popularityNorm) * popularityPenalty
        return score * (1.0 - penalty.coerceIn(0.0, 0.3)) // Max 30% penalty
    }
    
    /**
     * Apply diversity filtering to ensure varied recommendations
     */
    private fun applyDiversityFiltering(
        candidates: List<RecommendationCandidate>,
        limit: Int
    ): List<RecommendationCandidate> {
        val selected = mutableListOf<RecommendationCandidate>()
        val usedCategories = mutableSetOf<String>()
        val usedLocations = mutableSetOf<String>()
        
        // First pass: select top candidates with diverse categories/locations
        candidates.forEach { candidate ->
            val place = candidate.place
            if (place != null && selected.size < limit) {
                val category = place.category ?: "기타"
                val locationArea = extractLocationArea(place.location)
                
                // Allow some repetition but prefer diversity
                val categoryCount = selected.count { extractCategory(it.place) == category }
                val locationCount = selected.count { extractLocationArea(it.place?.location) == locationArea }
                
                if (categoryCount < 3 && locationCount < 2) {
                    selected.add(candidate)
                    usedCategories.add(category)
                    usedLocations.add(locationArea)
                }
            }
        }
        
        // Second pass: fill remaining slots with highest-scoring candidates
        candidates.forEach { candidate ->
            if (selected.size < limit && candidate !in selected) {
                selected.add(candidate)
            }
        }
        
        return selected.take(limit)
    }
    
    /**
     * Extract category from place for diversity filtering
     */
    private fun extractCategory(place: Place?): String {
        return place?.category ?: "기타"
    }
    
    /**
     * Extract location area (district) from full address for diversity filtering
     */
    private fun extractLocationArea(location: String?): String {
        if (location == null) return "기타"
        
        // Simple extraction of Seoul district from address
        val districtRegex = """([가-힣]+[구군])""".toRegex()
        val match = districtRegex.find(location)
        return match?.value ?: location.take(10) // Use first 10 chars as fallback
    }
    
    /**
     * Build the final recommendation response list with MBTI descriptions
     */
    private fun buildRecommendationResponseList(
        candidates: List<RecommendationCandidate>,
        user: User
    ): List<EnhancedPlaceRecommendation> {
        return candidates.mapNotNull { candidate ->
            val place = candidate.place
            if (place == null) return@mapNotNull null
            
            // Get MBTI-specific description
            val mbtiDescription = if (user.mbti != null) {
                placeMbtiDescriptionRepository.findByPlaceIdAndMbti(place.id!!, user.mbti)?.description
            } else null
            
            // Check if bookmarked
            val isBookmarked = bookmarkRepository.existsByUserAndPlace(user, place)
            
            EnhancedPlaceRecommendation(
                id = place.id.toString(),
                title = place.title,
                rating = place.rating.toDouble(),
                reviewCount = place.reviewCount,
                location = place.location ?: "",
                image = place.imageUrl,
                tags = place.tags,
                description = mbtiDescription ?: place.description,
                mbtiDescription = mbtiDescription,
                transportation = TransportationInfo(
                    car = place.transportationCarTime,
                    bus = place.transportationBusTime
                ),
                isBookmarked = isBookmarked,
                recommendationScore = BigDecimal(candidate.score).setScale(4, RoundingMode.HALF_UP),
                recommendationReasons = candidate.reasons,
                category = place.category
            )
        }
    }
    
    /**
     * Rating-based recommendations for users with no bookmarks or when similarity fails
     * Uses only places that meet the quality criteria (rating ≥ 3.0 or < 6 months old)
     */
    private fun getRatingBasedRecommendations(
        user: User, 
        limit: Int, 
        category: String? = null
    ): EnhancedRecommendationsResponse {
        logger.debug("Generating rating-based recommendations for user ${user.id}")
        
        // Get recommendable places with proper filtering
        val pageable = org.springframework.data.domain.PageRequest.of(0, limit * 2) // Get more for filtering
        val places = if (category != null) {
            placeRepository.findRecommendablePlaces(pageable).content
                .filter { it.category == category }
        } else {
            placeRepository.findRecommendablePlaces(pageable).content
        }
        
        // Apply diversity if enabled
        val selectedPlaces = if (diversityEnabled) {
            applyDiversityToPlaces(places, limit)
        } else {
            places.take(limit)
        }
        
        val recommendations = selectedPlaces.map { place ->
            val mbtiDescription = if (user.mbti != null) {
                placeMbtiDescriptionRepository.findByPlaceIdAndMbti(place.id!!, user.mbti)?.description
            } else null
            
            val isBookmarked = bookmarkRepository.existsByUserAndPlace(user, place)
            
            EnhancedPlaceRecommendation(
                id = place.id.toString(),
                title = place.title,
                rating = place.rating.toDouble(),
                reviewCount = place.reviewCount,
                location = place.location ?: "",
                image = place.imageUrl,
                tags = place.tags,
                description = mbtiDescription ?: place.description,
                mbtiDescription = mbtiDescription,
                transportation = TransportationInfo(
                    car = place.transportationCarTime,
                    bus = place.transportationBusTime
                ),
                isBookmarked = isBookmarked,
                recommendationScore = place.rating,
                recommendationReasons = if (place.isNewPlace) {
                    listOf("New place worth trying")
                } else {
                    listOf("High-rated place (${place.rating}/5.0)")
                },
                category = place.category
            )
        }
        
        return EnhancedRecommendationsResponse(
            recommendations = recommendations,
            algorithm = "rating_based_filtered",
            totalCount = recommendations.size,
            userMbti = user.mbti,
            basedOnBookmarks = 0
        )
    }
    
    /**
     * Apply diversity filtering to a list of places
     */
    private fun applyDiversityToPlaces(places: List<Place>, limit: Int): List<Place> {
        val selected = mutableListOf<Place>()
        val usedCategories = mutableSetOf<String>()
        val usedLocations = mutableSetOf<String>()
        
        // First pass: select diverse places
        places.forEach { place ->
            if (selected.size < limit) {
                val category = place.category ?: "기타"
                val locationArea = extractLocationArea(place.location)
                
                val categoryCount = selected.count { (it.category ?: "기타") == category }
                val locationCount = selected.count { extractLocationArea(it.location) == locationArea }
                
                if (categoryCount < 3 && locationCount < 2) {
                    selected.add(place)
                    usedCategories.add(category)
                    usedLocations.add(locationArea)
                }
            }
        }
        
        // Second pass: fill remaining slots with highest-rated
        places.forEach { place ->
            if (selected.size < limit && place !in selected) {
                selected.add(place)
            }
        }
        
        return selected.take(limit)
    }
    
    /**
     * Fallback recommendations for users with no bookmarks or when similarity calculation fails
     */
    private fun getFallbackRecommendations(user: User, limit: Int): EnhancedRecommendationsResponse {
        logger.debug("Generating fallback recommendations for user ${user.id}")
        
        val places = placeRepository.findTopRatedPlaces(4.0, org.springframework.data.domain.PageRequest.of(0, limit))
            .content
        
        val recommendations = places.map { place ->
            val mbtiDescription = if (user.mbti != null) {
                placeMbtiDescriptionRepository.findByPlaceIdAndMbti(place.id!!, user.mbti)?.description
            } else null
            
            val isBookmarked = bookmarkRepository.existsByUserAndPlace(user, place)
            
            EnhancedPlaceRecommendation(
                id = place.id.toString(),
                title = place.title,
                rating = place.rating.toDouble(),
                reviewCount = place.reviewCount,
                location = place.location ?: "",
                image = place.imageUrl,
                tags = place.tags,
                description = mbtiDescription ?: place.description,
                mbtiDescription = mbtiDescription,
                transportation = TransportationInfo(
                    car = place.transportationCarTime,
                    bus = place.transportationBusTime
                ),
                isBookmarked = isBookmarked,
                recommendationScore = place.rating.setScale(4, RoundingMode.HALF_UP),
                recommendationReasons = listOf("High-rated popular place"),
                category = place.category
            )
        }
        
        return EnhancedRecommendationsResponse(
            recommendations = recommendations,
            algorithm = "popularity_based_fallback",
            totalCount = recommendations.size,
            userMbti = user.mbti,
            basedOnBookmarks = 0
        )
    }
}

/**
 * Internal data class for recommendation candidates during processing
 */
private data class RecommendationCandidate(
    val placeId: Long,
    val score: Double,
    val reasons: List<String>,
    val place: Place?
)

/**
 * Enhanced recommendation response with algorithm transparency
 */
data class EnhancedRecommendationsResponse(
    val recommendations: List<EnhancedPlaceRecommendation>,
    val algorithm: String,
    val totalCount: Int,
    val userMbti: String?,
    val basedOnBookmarks: Int
)

/**
 * Enhanced place recommendation with MBTI-specific descriptions and scoring details
 */
data class EnhancedPlaceRecommendation(
    val id: String,
    val title: String,
    val rating: Double?,
    val reviewCount: Int,
    val location: String,
    val image: String?,
    val tags: List<String>,
    val description: String?,
    val mbtiDescription: String?,
    val transportation: TransportationInfo,
    val isBookmarked: Boolean,
    val recommendationScore: BigDecimal,
    val recommendationReasons: List<String>,
    val category: String?
)
package com.mohe.spring.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.mohe.spring.entity.*
import com.mohe.spring.repository.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Pageable
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.time.OffsetDateTime
import kotlin.math.sqrt

@Service
@Transactional
class VectorSimilarityService(
    private val userPreferenceVectorRepository: UserPreferenceVectorRepository,
    private val placeDescriptionVectorRepository: PlaceDescriptionVectorRepository,
    private val vectorSimilarityRepository: VectorSimilarityRepository,
    private val userRepository: UserRepository,
    private val placeRepository: PlaceRepository,
    private val keywordExtractionService: KeywordExtractionService,
    private val objectMapper: ObjectMapper,
    
    @Value("\${mohe.weight.jaccard:0.7}") private val jaccardWeight: Double,
    @Value("\${mohe.weight.cosine:0.3}") private val cosineWeight: Double,
    @Value("\${mohe.weight.mbtiBoost:0.3}") private val mbtiBoost: Double,
    @Value("\${mohe.recommendation.topK:100}") private val topK: Int
) {
    
    private val logger = LoggerFactory.getLogger(VectorSimilarityService::class.java)
    
    /**
     * Generate or update user preference vector from profile and preferences
     */
    @Transactional
    fun generateUserPreferenceVector(userId: Long, forceRegeneration: Boolean = false): UserPreferenceVector {
        val user = userRepository.findById(userId)
            .orElseThrow { IllegalArgumentException("User not found: $userId") }
            
        // Check if vector already exists and is recent
        if (!forceRegeneration) {
            val existingVector = userPreferenceVectorRepository.findByUserId(userId)
            if (existingVector != null && existingVector.createdAt.isAfter(OffsetDateTime.now().minusDays(7))) {
                logger.debug("Using existing user preference vector for user: $userId")
                return existingVector
            }
        }
        
        logger.info("Generating user preference vector for user: $userId")
        
        // Build combined text from user profile and preferences
        val combinedText = buildUserProfileText(user)
        
        // Extract keywords using Ollama
        val extractionResult = keywordExtractionService.extractKeywords(
            placeId = 0L, // User profile mode
            placeName = "user-${user.id}",
            placeDescription = combinedText,
            category = "user-profile"
        )
        
        // Create vector entity
        val userVector = UserPreferenceVector(
            user = user,
            rawProfileText = combinedText,
            combinedPreferencesText = combinedText,
            preferenceVector = extractionResult.vectorArray.joinToString(",", "[", "]"),
            selectedKeywords = objectMapper.valueToTree(extractionResult.selectedKeywords),
            extractionSource = "ollama-api",
            modelName = "keyword-extraction-model"
        )
        
        // Save or update vector
        val existing = userPreferenceVectorRepository.findByUserId(userId)
        if (existing != null) {
            userPreferenceVectorRepository.delete(existing)
        }
        
        val savedVector = userPreferenceVectorRepository.save(userVector)
        
        // Invalidate cached similarities for this user
        vectorSimilarityRepository.deleteByUserId(userId)
        
        logger.info("Generated user preference vector for user: $userId with ${extractionResult.selectedKeywords.size} keywords")
        return savedVector
    }
    
    /**
     * Generate or update place description vector
     */
    @Transactional  
    fun generatePlaceDescriptionVector(placeId: Long, forceRegeneration: Boolean = false): PlaceDescriptionVector {
        val place = placeRepository.findById(placeId)
            .orElseThrow { IllegalArgumentException("Place not found: $placeId") }
            
        // Check if vector already exists and is recent
        if (!forceRegeneration) {
            val existingVector = placeDescriptionVectorRepository.findByPlaceId(placeId)
            if (existingVector != null && existingVector.createdAt.isAfter(OffsetDateTime.now().minusDays(30))) {
                logger.debug("Using existing place description vector for place: $placeId")
                return existingVector
            }
        }
        
        logger.info("Generating place description vector for place: $placeId")
        
        // Build combined text from place attributes
        val combinedText = buildPlaceDescriptionText(place)
        
        // Extract keywords using Ollama
        val extractionResult = keywordExtractionService.extractKeywords(
            placeId = place.id ?: 0L,
            placeName = place.name,
            placeDescription = combinedText,
            category = place.category ?: "unknown"
        )
        
        // Create vector entity
        val placeVector = PlaceDescriptionVector(
            place = place,
            rawDescriptionText = combinedText,
            combinedAttributesText = combinedText,
            descriptionVector = extractionResult.vectorArray.joinToString(",", "[", "]"),
            selectedKeywords = objectMapper.valueToTree(extractionResult.selectedKeywords),
            extractionSource = "ollama-api",
            modelName = "keyword-extraction-model"
        )
        
        // Save or update vector
        val existing = placeDescriptionVectorRepository.findByPlaceId(placeId)
        if (existing != null) {
            placeDescriptionVectorRepository.delete(existing)
        }
        
        val savedVector = placeDescriptionVectorRepository.save(placeVector)
        
        // Invalidate cached similarities for this place
        vectorSimilarityRepository.deleteByPlaceId(placeId)
        
        logger.info("Generated place description vector for place: $placeId with ${extractionResult.selectedKeywords.size} keywords")
        return savedVector
    }
    
    /**
     * Calculate similarity between user and place vectors
     */
    @Transactional
    fun calculateUserPlaceSimilarity(userId: Long, placeId: Long, useCache: Boolean = true): VectorSimilarity {
        // Check cache first if requested
        if (useCache) {
            val cachedSimilarity = vectorSimilarityRepository.findByUserIdAndPlaceId(userId, placeId)
            if (cachedSimilarity?.isRecent(24) == true) {
                logger.debug("Using cached similarity for user: $userId, place: $placeId")
                return cachedSimilarity
            }
        }
        
        logger.debug("Calculating vector similarity for user: $userId, place: $placeId")
        
        // Get or generate vectors
        val userVector = getUserPreferenceVector(userId)
        val placeVector = getPlaceDescriptionVector(placeId)
        
        // Calculate similarity
        val similarityResult = placeVector.calculateSimilarityWithUser(userVector, mbtiBoost)
        
        // Create and save similarity record
        val vectorSimilarity = VectorSimilarity.fromCalculationResult(
            userId = userId,
            placeId = placeId,
            result = similarityResult,
            userVectorVersion = userVector.id,
            placeVectorVersion = placeVector.id
        )
        
        return vectorSimilarityRepository.save(vectorSimilarity)
    }
    
    /**
     * Get top similar places for a user using vector similarity
     */
    @Transactional(readOnly = true)
    fun getTopSimilarPlacesForUser(
        userId: Long, 
        limit: Int = topK,
        excludePlaceIds: List<Long> = emptyList(),
        minSimilarityThreshold: Double = 0.1
    ): List<PlaceVectorMatch> {
        
        logger.debug("Finding top $limit similar places for user: $userId")
        
        // Get user vector
        val userVector = getUserPreferenceVector(userId)
        
        // Get all place vectors (or use efficient vector search if implemented)
        val allPlaceVectors = placeDescriptionVectorRepository.findAllActive()
        
        val similarities = mutableListOf<PlaceVectorMatch>()
        
        allPlaceVectors.forEach { placeVector ->
            if (placeVector.place.id !in excludePlaceIds) {
                try {
                    // Calculate or get cached similarity
                    val similarity = calculateUserPlaceSimilarity(userId, placeVector.place.id!!, useCache = true)
                    
                    if (similarity.getWeightedSimilarityAsDouble() >= minSimilarityThreshold) {
                        similarities.add(
                            PlaceVectorMatch(
                                place = placeVector.place,
                                similarity = similarity,
                                matchingKeywords = getMatchingKeywords(userVector, placeVector),
                                recommendationReason = generateRecommendationReason(userVector, placeVector, similarity)
                            )
                        )
                    }
                    
                } catch (e: Exception) {
                    logger.warn("Failed to calculate similarity for place: ${placeVector.place.id}", e)
                }
            }
        }
        
        // Sort by weighted similarity and return top results
        return similarities
            .sortedByDescending { it.similarity.getWeightedSimilarityAsDouble() }
            .take(limit)
    }
    
    /**
     * Batch calculate similarities for all user-place combinations
     */
    @Async("vectorSimilarityExecutor")
    @Transactional
    fun batchCalculateSimilarities(batchSize: Int = 1000) {
        logger.info("Starting batch similarity calculation with batch size: $batchSize")
        
        val totalUsers = userRepository.count()
        val totalPlaces = placeRepository.count()
        logger.info("Processing similarities for $totalUsers users and $totalPlaces places")
        
        var processedCount = 0
        var page = 0
        
        while (true) {
            val users = userRepository.findAll(PageRequest.of(page, batchSize))
            if (users.isEmpty) break
            
            users.content.forEach { user ->
                try {
                    // Ensure user has a vector
                    getUserPreferenceVector(user.id)
                    
                    // Calculate similarities for top places only to avoid overwhelming the system
                    val topPlaces = placeRepository.findTopRatedPlaces(3.0, PageRequest.of(0, 200))
                    
                    topPlaces.content.forEach { place ->
                        try {
                            calculateUserPlaceSimilarity(user.id, place.id!!, useCache = true)
                            processedCount++
                            
                            if (processedCount % 1000 == 0) {
                                logger.info("Processed $processedCount similarities...")
                            }
                            
                        } catch (e: Exception) {
                            logger.warn("Failed to calculate similarity for user: ${user.id}, place: ${place.id}", e)
                        }
                    }
                    
                } catch (e: Exception) {
                    logger.warn("Failed to process user: ${user.id}", e)
                }
            }
            
            page++
        }
        
        logger.info("Completed batch similarity calculation: $processedCount similarities processed")
    }
    
    /**
     * Get or generate user preference vector
     */
    private fun getUserPreferenceVector(userId: Long): UserPreferenceVector {
        return userPreferenceVectorRepository.findByUserId(userId) 
            ?: generateUserPreferenceVector(userId, forceRegeneration = true)
    }
    
    /**
     * Get or generate place description vector  
     */
    private fun getPlaceDescriptionVector(placeId: Long): PlaceDescriptionVector {
        return placeDescriptionVectorRepository.findByPlaceId(placeId)
            ?: generatePlaceDescriptionVector(placeId, forceRegeneration = true)
    }
    
    /**
     * Build combined text from user profile for vectorization
     */
    private fun buildUserProfileText(user: User): String {
        val textParts = mutableListOf<String>()
        
        // Basic profile
        user.mbti?.let { textParts.add("MBTI personality type: $it") }
        user.ageRange?.let { textParts.add("Age range: $it") }
        user.transportation?.let { textParts.add("Preferred transportation: $it") }
        
        // User preferences from preferences table
        user.preferences.forEach { pref ->
            textParts.add("${pref.prefKey}: ${pref.prefValue}")
        }
        
        // Recent places for context (places they've bookmarked or visited)
        val recentPlaceTypes = user.recentViews.take(10).mapNotNull { it.place.category }.distinct()
        if (recentPlaceTypes.isNotEmpty()) {
            textParts.add("Recently visited place types: ${recentPlaceTypes.joinToString(", ")}")
        }
        
        val bookmarkedPlaceTypes = user.bookmarks.take(10).mapNotNull { it.place.category }.distinct()
        if (bookmarkedPlaceTypes.isNotEmpty()) {
            textParts.add("Bookmarked place types: ${bookmarkedPlaceTypes.joinToString(", ")}")
        }
        
        return textParts.joinToString(". ").ifEmpty { "User with ${user.mbti ?: "unknown"} preferences" }
    }
    
    /**
     * Build combined text from place attributes for vectorization
     */
    private fun buildPlaceDescriptionText(place: Place): String {
        val textParts = mutableListOf<String>()
        
        // Basic info
        textParts.add("Place name: ${place.name}")
        place.category?.let { textParts.add("Category: $it") }
        place.description?.let { textParts.add("Description: $it") }
        
        // Location context
        place.location?.let { textParts.add("Location: $it") }
        
        // Amenities and features
        if (place.amenities.isNotEmpty()) {
            textParts.add("Amenities: ${place.amenities.joinToString(", ")}")
        }
        
        if (place.tags.isNotEmpty()) {
            textParts.add("Tags: ${place.tags.joinToString(", ")}")
        }
        
        // Operational details
        place.operatingHours?.let { textParts.add("Operating hours: $it") }
        
        // Transportation
        place.transportationCarTime?.let { textParts.add("Car access: $it") }
        place.transportationBusTime?.let { textParts.add("Public transport: $it") }
        
        // Rating and popularity context
        if (place.rating > BigDecimal.ZERO) {
            textParts.add("Rating: ${place.rating}/5.0 with ${place.reviewCount} reviews")
        }
        
        return textParts.joinToString(". ").ifEmpty { "Place: ${place.name}" }
    }
    
    /**
     * Get matching keywords between user and place vectors
     */
    private fun getMatchingKeywords(userVector: UserPreferenceVector, placeVector: PlaceDescriptionVector): List<String> {
        val userKeywords = userVector.getSelectedKeywordsList().map { it.keyword }.toSet()
        val placeKeywords = placeVector.getSelectedKeywordsList().map { it.keyword }.toSet()
        return userKeywords.intersect(placeKeywords).toList()
    }
    
    /**
     * Generate human-readable recommendation reason
     */
    private fun generateRecommendationReason(
        userVector: UserPreferenceVector, 
        placeVector: PlaceDescriptionVector,
        similarity: VectorSimilarity
    ): String {
        val matchingKeywords = getMatchingKeywords(userVector, placeVector)
        val userMbti = userVector.user.mbti
        val similarityScore = similarity.getWeightedSimilarityAsDouble()
        
        return when {
            similarityScore > 0.8 && matchingKeywords.size >= 5 -> {
                "Perfect match! Shares ${matchingKeywords.size} key preferences including ${matchingKeywords.take(3).joinToString(", ")}"
            }
            similarityScore > 0.6 && userMbti != null -> {
                "Great fit for ${userMbti} personality - matches your preferences for ${matchingKeywords.take(2).joinToString(" and ")}"
            }
            similarityScore > 0.4 -> {
                "Good match based on your interest in ${matchingKeywords.take(2).joinToString(" and ")}"
            }
            else -> {
                "Recommended based on similar user preferences"
            }
        }
    }
    
    /**
     * Clean up old similarity calculations
     */
    @Async("vectorSimilarityExecutor")
    @Transactional
    fun cleanupOldSimilarities(olderThanDays: Long = 30) {
        logger.info("Cleaning up similarity calculations older than $olderThanDays days")
        
        val cutoffDate = OffsetDateTime.now().minusDays(olderThanDays)
        val deletedCount = vectorSimilarityRepository.deleteOlderThan(cutoffDate)
        
        logger.info("Cleaned up $deletedCount old similarity calculations")
    }
}

/**
 * Result of place-user vector matching
 */
data class PlaceVectorMatch(
    val place: Place,
    val similarity: VectorSimilarity,
    val matchingKeywords: List<String>,
    val recommendationReason: String
) {
    fun getSimilarityScore(): Double = similarity.getWeightedSimilarityAsDouble()
}


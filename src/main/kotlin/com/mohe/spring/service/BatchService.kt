package com.mohe.spring.service

import com.mohe.spring.controller.*
import com.mohe.spring.entity.Place
import com.mohe.spring.entity.PlaceExternalRaw
import com.mohe.spring.entity.ExternalDataSource
import com.mohe.spring.repository.PlaceRepository
import com.mohe.spring.repository.UserRepository
import com.mohe.spring.repository.PlaceMbtiDescriptionRepository
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.time.LocalDateTime
import java.time.OffsetDateTime

@Service
class BatchService(
    private val placeRepository: PlaceRepository,
    private val userRepository: UserRepository,
    private val placeMbtiDescriptionRepository: PlaceMbtiDescriptionRepository,
    private val ollamaService: OllamaService,
    private val keywordExtractionService: KeywordExtractionService,
    private val objectMapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(BatchService::class.java)

    @Transactional
    fun ingestPlaceData(placeDataList: List<BatchPlaceRequest>): BatchPlaceResponse {
        logger.info("Processing ${placeDataList.size} place records from batch")
        
        var insertedCount = 0
        var updatedCount = 0
        var skippedCount = 0
        var errorCount = 0
        val errors = mutableListOf<String>()

        placeDataList.forEach { placeData ->
            try {
                val result = processPlaceRecord(placeData)
                when (result) {
                    ProcessResult.INSERTED -> insertedCount++
                    ProcessResult.UPDATED -> updatedCount++
                    ProcessResult.SKIPPED -> skippedCount++
                }
            } catch (e: Exception) {
                logger.error("Failed to process place with external ID ${placeData.externalId}: ${e.message}", e)
                errorCount++
                errors.add("External ID ${placeData.externalId}: ${e.message}")
            }
        }

        logger.info("Batch place processing completed: $insertedCount inserted, $updatedCount updated, $skippedCount skipped, $errorCount errors")
        
        return BatchPlaceResponse(
            processedCount = placeDataList.size,
            insertedCount = insertedCount,
            updatedCount = updatedCount,
            skippedCount = skippedCount,
            errorCount = errorCount,
            errors = errors
        )
    }

    @Transactional
    fun ingestUserData(userDataList: List<BatchUserRequest>): BatchUserResponse {
        logger.info("Processing ${userDataList.size} user records from batch")
        
        var insertedCount = 0
        var updatedCount = 0
        var skippedCount = 0
        var errorCount = 0
        val errors = mutableListOf<String>()

        userDataList.forEach { userData ->
            try {
                val result = processUserRecord(userData)
                when (result) {
                    ProcessResult.INSERTED -> insertedCount++
                    ProcessResult.UPDATED -> updatedCount++
                    ProcessResult.SKIPPED -> skippedCount++
                }
            } catch (e: Exception) {
                logger.error("Failed to process user with external ID ${userData.externalId}: ${e.message}", e)
                errorCount++
                errors.add("External ID ${userData.externalId}: ${e.message}")
            }
        }

        logger.info("Batch user processing completed: $insertedCount inserted, $updatedCount updated, $skippedCount skipped, $errorCount errors")
        
        return BatchUserResponse(
            processedCount = userDataList.size,
            insertedCount = insertedCount,
            updatedCount = updatedCount,
            skippedCount = skippedCount,
            errorCount = errorCount,
            errors = errors
        )
    }

    private fun processPlaceRecord(placeData: BatchPlaceRequest): ProcessResult {
        // Validate the data
        validatePlaceData(placeData)

        return try {
            // Check if place already exists by name (old method - for backward compatibility)
            val existingPlace = placeRepository.findByName(placeData.name).orElse(null)

            if (existingPlace != null) {
                val existing = existingPlace
                
                // Only update if external data is newer
                // Since Place entity may not have externalUpdatedAt, we'll always update for now
                val updatedPlace = existing.copy(
                    name = placeData.name,
                    description = placeData.description ?: existing.description,
                    category = placeData.category ?: existing.category,
                    address = placeData.address ?: existing.address,
                    latitude = placeData.latitude?.let { BigDecimal(it.toString()) } ?: existing.latitude,
                    longitude = placeData.longitude?.let { BigDecimal(it.toString()) } ?: existing.longitude,
                    phone = placeData.phone ?: existing.phone,
                    rating = placeData.rating?.let { BigDecimal(it.toString()) } ?: existing.rating,
                    tags = placeData.tags ?: existing.tags,
                    operatingHours = placeData.hours ?: existing.operatingHours
                )
                
                placeRepository.save(updatedPlace)
                logger.debug("Updated place: ${placeData.name}")
                ProcessResult.UPDATED
            } else {
                // Create new place
                val newPlace = Place(
                    id = null,
                    name = placeData.name,
                    description = placeData.description,
                    category = placeData.category,
                    address = placeData.address,
                    latitude = placeData.latitude?.let { BigDecimal(it.toString()) },
                    longitude = placeData.longitude?.let { BigDecimal(it.toString()) },
                    phone = placeData.phone,
                    rating = placeData.rating?.let { BigDecimal(it.toString()) } ?: BigDecimal.ZERO,
                    tags = placeData.tags ?: emptyList(),
                    operatingHours = placeData.hours,
                    createdAt = OffsetDateTime.now()
                )
                
                placeRepository.save(newPlace)
                logger.debug("Inserted new place: ${placeData.name}")
                ProcessResult.INSERTED
            }
        } catch (ex: DataIntegrityViolationException) {
            logger.warn("Data integrity violation for place ${placeData.name}: ${ex.message}")
            ProcessResult.SKIPPED
        }
    }

    @Transactional
    fun ingestPlacesFromBatch(placeDataList: List<InternalPlaceIngestRequest>): InternalPlaceIngestResponse {
        logger.info("Processing ${placeDataList.size} enriched place records from batch with Ollama keyword extraction")
        
        var insertedCount = 0
        var updatedCount = 0
        var skippedCount = 0
        var errorCount = 0
        var keywordGeneratedCount = 0
        val errors = mutableListOf<String>()

        placeDataList.forEach { placeData ->
            try {
                val result = processEnrichedPlaceRecord(placeData)
                when (result.first) {
                    ProcessResult.INSERTED -> {
                        insertedCount++
                        if (result.second) keywordGeneratedCount++
                    }
                    ProcessResult.UPDATED -> {
                        updatedCount++
                        if (result.second) keywordGeneratedCount++
                    }
                    ProcessResult.SKIPPED -> skippedCount++
                }
            } catch (e: Exception) {
                logger.error("Failed to process place with Naver ID ${placeData.naverPlaceId}: ${e.message}", e)
                errorCount++
                errors.add("Naver ID ${placeData.naverPlaceId}: ${e.message}")
            }
        }

        logger.info("Batch place processing completed: $insertedCount inserted, $updatedCount updated, $skippedCount skipped, $errorCount errors, $keywordGeneratedCount keywords generated")
        
        return InternalPlaceIngestResponse(
            processedCount = placeDataList.size,
            insertedCount = insertedCount,
            updatedCount = updatedCount,
            skippedCount = skippedCount,
            errorCount = errorCount,
            keywordGeneratedCount = keywordGeneratedCount,
            errors = errors
        )
    }

    private fun processEnrichedPlaceRecord(placeData: InternalPlaceIngestRequest): Pair<ProcessResult, Boolean> {
        // Validate the enriched place data
        validateEnrichedPlaceData(placeData)

        return try {
            // Check if place already exists by external IDs
            val existingPlace = findExistingPlace(placeData)
            
            val place = if (existingPlace != null) {
                // Update existing place
                val updatedPlace = updatePlaceFromEnrichedData(existingPlace, placeData)
                placeRepository.save(updatedPlace)
                logger.debug("Updated place: ${placeData.name}")
                updatedPlace
            } else {
                // Create new place
                val newPlace = createPlaceFromEnrichedData(placeData)
                val savedPlace = placeRepository.save(newPlace)
                logger.debug("Inserted new place: ${placeData.name}")
                savedPlace
            }
            
            // Store external raw data for audit
            storeExternalRawData(place.id!!, placeData)
            
            // Generate keyword extraction using Ollama
            val keywordGenerated = generateKeywordExtractionForPlace(place)
            
            val result = if (existingPlace != null) ProcessResult.UPDATED else ProcessResult.INSERTED
            Pair(result, keywordGenerated)
            
        } catch (ex: DataIntegrityViolationException) {
            logger.warn("Data integrity violation for place ${placeData.name}: ${ex.message}")
            Pair(ProcessResult.SKIPPED, false)
        }
    }

    private fun processUserRecord(userData: BatchUserRequest): ProcessResult {
        // Validate the data
        validateUserData(userData)

        return try {
            // Check if user already exists by email
            val existingUser = userRepository.findByEmail(userData.email)

            if (existingUser.isPresent) {
                val existing = existingUser.get()
                
                // Update existing user with new information
                val updatedUser = existing.copy(
                    nickname = userData.name
                    // Note: We don't update email as it's the primary identifier
                    // phone = userData.phone, // Uncomment if User entity has phone field
                )
                
                userRepository.save(updatedUser)
                logger.debug("Updated user: ${userData.email}")
                ProcessResult.UPDATED
            } else {
                // For now, we'll skip creating new users via batch as it requires password
                // and other required fields that aren't provided by external data
                logger.debug("Skipped creating new user from external data: ${userData.email}")
                ProcessResult.SKIPPED
            }
        } catch (ex: DataIntegrityViolationException) {
            logger.warn("Data integrity violation for user ${userData.email}: ${ex.message}")
            ProcessResult.SKIPPED
        }
    }
    
    private fun findExistingPlace(placeData: InternalPlaceIngestRequest): Place? {
        // Try to find by Naver place ID first
        placeData.naverPlaceId.let { naverPlaceId ->
            placeRepository.findByNaverPlaceId(naverPlaceId)?.let { return it }
        }
        
        // Try to find by Google place ID
        placeData.googlePlaceId?.let { googlePlaceId ->
            placeRepository.findByGooglePlaceId(googlePlaceId)?.let { return it }
        }
        
        // Try to find by name and coordinates (fuzzy match)
        if (placeData.latitude != null && placeData.longitude != null) {
            return placeRepository.findSimilarPlace(
                placeData.name, 
                placeData.latitude, 
                placeData.longitude, 
                BigDecimal("0.001") // ~100m radius
            )
        }
        
        return null
    }
    
    private fun createPlaceFromEnrichedData(placeData: InternalPlaceIngestRequest): Place {
        // Create minimal Place entity that should definitely work
        return Place(
            name = placeData.name,
            title = placeData.name,
            address = placeData.address,
            naverPlaceId = placeData.naverPlaceId,
            category = placeData.category,
            description = placeData.description,
            latitude = placeData.latitude,
            longitude = placeData.longitude,
            imageUrl = placeData.imageUrl,
            rating = placeData.rating?.let { BigDecimal(it.toString()) } ?: BigDecimal.ZERO
        )
    }
    
    private fun updatePlaceFromEnrichedData(existing: Place, placeData: InternalPlaceIngestRequest): Place {
        return existing.copy(
            name = placeData.name,
            title = placeData.name,
            address = placeData.address ?: existing.address,
            roadAddress = placeData.roadAddress ?: existing.roadAddress,
            latitude = placeData.latitude ?: existing.latitude,
            longitude = placeData.longitude ?: existing.longitude,
            category = placeData.category ?: existing.category,
            description = placeData.description.takeIf { it.isNotBlank() } ?: existing.description,
            imageUrl = placeData.imageUrl ?: existing.imageUrl,
            rating = placeData.rating?.let { BigDecimal(it.toString()) } ?: existing.rating,
            naverPlaceId = placeData.naverPlaceId,
            googlePlaceId = placeData.googlePlaceId ?: existing.googlePlaceId,
            phone = placeData.phone ?: existing.phone,
            websiteUrl = placeData.websiteUrl ?: existing.websiteUrl,
            openingHours = placeData.openingHours?.let { objectMapper.readTree(it) } ?: existing.openingHours,
            types = if (placeData.types.isNotEmpty()) placeData.types else existing.types,
            userRatingsTotal = placeData.userRatingsTotal ?: existing.userRatingsTotal,
            priceLevel = placeData.priceLevel?.toShort() ?: existing.priceLevel,
            sourceFlags = objectMapper.valueToTree(placeData.sourceFlags),
            keywordVector = formatVectorForDatabase(placeData.keywordVector),
            updatedAt = LocalDateTime.now()
        )
    }
    
    private fun formatVectorForDatabase(vector: List<Double>): String? {
        return if (vector.isNotEmpty()) {
            "[${vector.joinToString(",")}]"
        } else null
    }
    
    private fun storeExternalRawData(placeId: Long, placeData: InternalPlaceIngestRequest) {
        try {
            // Store Naver raw data
            val naverRaw = PlaceExternalRaw(
                source = ExternalDataSource.NAVER.sourceName,
                externalId = placeData.naverPlaceId,
                placeId = placeId,
                payload = objectMapper.readTree(placeData.naverRawData),
                fetchedAt = LocalDateTime.now()
            )
            // Note: Would need PlaceExternalRawRepository to save
            
            // Store Google raw data if available
            placeData.googleRawData?.let { googleRawData ->
                placeData.googlePlaceId?.let { googlePlaceId ->
                    val googleRaw = PlaceExternalRaw(
                        source = ExternalDataSource.GOOGLE.sourceName,
                        externalId = googlePlaceId,
                        placeId = placeId,
                        payload = objectMapper.readTree(googleRawData),
                        fetchedAt = LocalDateTime.now()
                    )
                    // Note: Would need PlaceExternalRawRepository to save
                }
            }
        } catch (e: Exception) {
            logger.warn("Failed to store external raw data for place $placeId: ${e.message}")
        }
    }
    
    private fun generateKeywordExtractionForPlace(place: Place): Boolean {
        return try {
            // Build comprehensive place description for keyword extraction
            val contextDescription = buildContextDescription(place)
            
            val keywordResult = keywordExtractionService.extractKeywords(
                placeId = place.id!!,
                placeName = place.name,
                placeDescription = contextDescription,
                category = place.category ?: "",
                additionalContext = buildAdditionalContext(place)
            )
            
            logger.debug("Generated keyword extraction for place: ${place.name} - ${keywordResult.selectedKeywords.size} keywords extracted")
            true
        } catch (e: Exception) {
            logger.error("Failed to generate keyword extraction for place ${place.name}: ${e.message}", e)
            false
        }
    }
    
    private fun buildContextDescription(place: Place): String {
        val parts = mutableListOf<String>()
        
        // Main description
        place.description?.takeIf { it.isNotBlank() }?.let { parts.add(it) }
        
        // Category information
        place.category?.takeIf { it.isNotBlank() }?.let { parts.add("카테고리: $it") }
        
        // Location information
        place.address?.takeIf { it.isNotBlank() }?.let { parts.add("주소: $it") }
        
        // Rating information
        if (place.rating != null && place.rating > BigDecimal.ZERO) {
            val ratingText = when {
                place.rating >= BigDecimal("4.5") -> "매우 높은 평점의"
                place.rating >= BigDecimal("4.0") -> "높은 평점의"
                place.rating >= BigDecimal("3.5") -> "좋은 평점의"
                place.rating >= BigDecimal("3.0") -> "괜찮은 평점의"
                else -> "평점이 있는"
            }
            parts.add("$ratingText 장소 (${place.rating}/5.0)")
        }
        
        // Price level information
        place.priceLevel?.let { priceLevel ->
            val priceText = when (priceLevel.toInt()) {
                0 -> "무료"
                1 -> "저렴한"
                2 -> "적당한 가격의"
                3 -> "비싼"
                4 -> "매우 비싼"
                else -> "가격대가 있는"
            }
            parts.add("$priceText 장소")
        }
        
        return if (parts.isNotEmpty()) {
            parts.joinToString(". ")
        } else {
            "${place.name}은/는 ${place.category ?: "특별한"} 장소입니다."
        }
    }
    
    private fun buildAdditionalContext(place: Place): String {
        val context = mutableListOf<String>()
        
        // Google Places types
        place.types?.let { types ->
            if (types.isNotEmpty()) {
                context.add("유형: ${types.joinToString(", ")}")
            }
        }
        
        // Opening hours availability
        place.openingHours?.let { 
            context.add("영업시간 정보 있음")
        }
        
        // Website availability
        place.websiteUrl?.takeIf { it.isNotBlank() }?.let {
            context.add("웹사이트 있음")
        }
        
        // Phone availability
        place.phone?.takeIf { it.isNotBlank() }?.let {
            context.add("전화번호 있음")
        }
        
        // User ratings total
        place.userRatingsTotal?.let { ratingsTotal ->
            val popularityText = when {
                ratingsTotal >= 1000 -> "매우 인기 있는"
                ratingsTotal >= 500 -> "인기 있는"
                ratingsTotal >= 100 -> "알려진"
                ratingsTotal >= 10 -> "리뷰가 있는"
                else -> "새로운"
            }
            context.add("$popularityText 장소 (리뷰 ${ratingsTotal}개)")
        }
        
        return context.joinToString(", ")
    }
    
    private fun validateEnrichedPlaceData(placeData: InternalPlaceIngestRequest) {
        require(placeData.name.isNotBlank()) { "Place name cannot be blank" }
        require(placeData.naverPlaceId.isNotBlank()) { "Naver place ID cannot be blank" }
        
        // Validate coordinates if provided
        placeData.latitude?.let { lat ->
            require(lat >= BigDecimal("-90") && lat <= BigDecimal("90")) { 
                "Latitude must be between -90.0 and 90.0" 
            }
        }
        placeData.longitude?.let { lng ->
            require(lng >= BigDecimal("-180") && lng <= BigDecimal("180")) { 
                "Longitude must be between -180.0 and 180.0" 
            }
        }
        
        // Validate rating if provided
        placeData.rating?.let { rating ->
            require(rating >= 0.0 && rating <= 5.0) { "Rating must be between 0.0 and 5.0" }
        }
        
        // Validate price level if provided
        placeData.priceLevel?.let { priceLevel ->
            require(priceLevel >= 0 && priceLevel <= 4) { "Price level must be between 0 and 4" }
        }
    }

    private fun validatePlaceData(placeData: BatchPlaceRequest) {
        require(placeData.name.isNotBlank()) { "Place name cannot be blank" }
        require(placeData.externalId.isNotBlank()) { "External ID cannot be blank" }
        
        // Validate rating if provided
        placeData.rating?.let { rating ->
            require(rating >= 0.0 && rating <= 5.0) { "Rating must be between 0.0 and 5.0" }
        }
        
        // Validate coordinates if provided
        placeData.latitude?.let { lat ->
            require(lat >= -90.0 && lat <= 90.0) { "Latitude must be between -90.0 and 90.0" }
        }
        placeData.longitude?.let { lng ->
            require(lng >= -180.0 && lng <= 180.0) { "Longitude must be between -180.0 and 180.0" }
        }
    }

    private fun validateUserData(userData: BatchUserRequest) {
        require(userData.name.isNotBlank()) { "User name cannot be blank" }
        require(userData.email.isNotBlank()) { "Email cannot be blank" }
        require(userData.externalId.isNotBlank()) { "External ID cannot be blank" }
        
        // Validate email format
        val emailPattern = "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$".toRegex()
        require(emailPattern.matches(userData.email)) { "Invalid email format" }
        
        // Validate status
        val validStatuses = listOf("ACTIVE", "INACTIVE", "PENDING", "SUSPENDED")
        require(userData.status.uppercase() in validStatuses) { 
            "Status must be one of: ${validStatuses.joinToString(", ")}" 
        }
    }

    fun cleanupOldAndLowRatedPlaces(): DatabaseCleanupResponse {
        logger.info("Starting database cleanup of old and low-rated places")
        
        val messages = mutableListOf<String>()
        var removedCount = 0
        
        try {
            // Delete places that are old (>6 months) AND have low rating (<3.0)
            val oldDate = LocalDateTime.now().minusMonths(6)
            val lowRatingThreshold = BigDecimal("3.0")
            
            val oldLowRatedPlaces = placeRepository.findOldLowRatedPlaces(oldDate, lowRatingThreshold)
            
            if (oldLowRatedPlaces.isNotEmpty()) {
                placeRepository.deleteAll(oldLowRatedPlaces)
                removedCount = oldLowRatedPlaces.size
                messages.add("Removed $removedCount old places with rating < $lowRatingThreshold")
                logger.info("Removed $removedCount old low-rated places")
            } else {
                messages.add("No old low-rated places found to remove")
                logger.info("No old low-rated places found for cleanup")
            }
            
            // Optional: Clean up places without proper coordinates
            val placesWithoutCoordinates = placeRepository.findPlacesWithoutCoordinates()
            if (placesWithoutCoordinates.isNotEmpty()) {
                placeRepository.deleteAll(placesWithoutCoordinates)
                val coordCleanupCount = placesWithoutCoordinates.size
                removedCount += coordCleanupCount
                messages.add("Removed $coordCleanupCount places without valid coordinates")
                logger.info("Removed $coordCleanupCount places without coordinates")
            }
            
        } catch (ex: Exception) {
            logger.error("Error during database cleanup: ${ex.message}", ex)
            messages.add("Cleanup error: ${ex.message}")
        }
        
        return DatabaseCleanupResponse(removedCount, messages)
    }

    private enum class ProcessResult {
        INSERTED,
        UPDATED,
        SKIPPED
    }
}
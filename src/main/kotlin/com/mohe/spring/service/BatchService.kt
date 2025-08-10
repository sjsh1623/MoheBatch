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
            // Check if place already exists by external ID
            // For now, we'll use name as unique identifier since Place entity may not have externalId
            val existingPlace = placeRepository.findByName(placeData.name)

            if (existingPlace.isPresent) {
                val existing = existingPlace.get()
                
                // Only update if external data is newer
                // Since Place entity may not have externalUpdatedAt, we'll always update for now
                val updatedPlace = existing.copy(
                    name = placeData.name,
                    description = placeData.description ?: existing.description,
                    category = placeData.category ?: existing.category,
                    address = placeData.address ?: existing.address,
                    latitude = placeData.latitude ?: existing.latitude,
                    longitude = placeData.longitude ?: existing.longitude,
                    phone = placeData.phone ?: existing.phone,
                    website = placeData.website ?: existing.website,
                    rating = placeData.rating ?: existing.rating,
                    tags = placeData.tags?.let { it.joinToString(",") } ?: existing.tags,
                    hours = placeData.hours ?: existing.hours,
                    updatedAt = LocalDateTime.now()
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
                    latitude = placeData.latitude,
                    longitude = placeData.longitude,
                    phone = placeData.phone,
                    website = placeData.website,
                    rating = placeData.rating,
                    tags = placeData.tags?.joinToString(","),
                    hours = placeData.hours,
                    createdAt = LocalDateTime.now(),
                    updatedAt = LocalDateTime.now()
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
        logger.info("Processing ${placeDataList.size} enriched place records from batch with Ollama MBTI generation")
        
        var insertedCount = 0
        var updatedCount = 0
        var skippedCount = 0
        var errorCount = 0
        var mbtiGeneratedCount = 0
        val errors = mutableListOf<String>()

        placeDataList.forEach { placeData ->
            try {
                val result = processEnrichedPlaceRecord(placeData)
                when (result.first) {
                    ProcessResult.INSERTED -> {
                        insertedCount++
                        if (result.second) mbtiGeneratedCount++
                    }
                    ProcessResult.UPDATED -> {
                        updatedCount++
                        if (result.second) mbtiGeneratedCount++
                    }
                    ProcessResult.SKIPPED -> skippedCount++
                }
            } catch (e: Exception) {
                logger.error("Failed to process place with Naver ID ${placeData.naverPlaceId}: ${e.message}", e)
                errorCount++
                errors.add("Naver ID ${placeData.naverPlaceId}: ${e.message}")
            }
        }

        logger.info("Batch place processing completed: $insertedCount inserted, $updatedCount updated, $skippedCount skipped, $errorCount errors, $mbtiGeneratedCount MBTI generated")
        
        return InternalPlaceIngestResponse(
            processedCount = placeDataList.size,
            insertedCount = insertedCount,
            updatedCount = updatedCount,
            skippedCount = skippedCount,
            errorCount = errorCount,
            mbtiGeneratedCount = mbtiGeneratedCount,
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
            
            // Generate MBTI descriptions using Ollama
            val mbtiGenerated = generateMbtiDescriptionsForPlace(place)
            
            val result = if (existingPlace != null) ProcessResult.UPDATED else ProcessResult.INSERTED
            Pair(result, mbtiGenerated)
            
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
                    nickname = userData.name,
                    // Note: We don't update email as it's the primary identifier
                    // phone = userData.phone, // Uncomment if User entity has phone field
                    updatedAt = LocalDateTime.now()
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
        return Place(
            name = placeData.name,
            title = placeData.name,
            address = placeData.address,
            roadAddress = placeData.roadAddress,
            latitude = placeData.latitude,
            longitude = placeData.longitude,
            category = placeData.category,
            description = placeData.description,
            imageUrl = placeData.imageUrl,
            rating = placeData.rating?.let { BigDecimal(it.toString()) } ?: BigDecimal.ZERO,
            naverPlaceId = placeData.naverPlaceId,
            googlePlaceId = placeData.googlePlaceId,
            phone = placeData.phone,
            websiteUrl = placeData.websiteUrl,
            openingHours = placeData.openingHours?.let { objectMapper.readTree(it) },
            types = placeData.types.toTypedArray(),
            userRatingsTotal = placeData.userRatingsTotal,
            priceLevel = placeData.priceLevel?.toShort(),
            sourceFlags = objectMapper.valueToTree(placeData.sourceFlags),
            createdAt = OffsetDateTime.now(),
            updatedAt = LocalDateTime.now()
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
            types = if (placeData.types.isNotEmpty()) placeData.types.toTypedArray() else existing.types,
            userRatingsTotal = placeData.userRatingsTotal ?: existing.userRatingsTotal,
            priceLevel = placeData.priceLevel?.toShort() ?: existing.priceLevel,
            sourceFlags = objectMapper.valueToTree(placeData.sourceFlags),
            updatedAt = LocalDateTime.now()
        )
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
    
    private fun generateMbtiDescriptionsForPlace(place: Place): Boolean {
        return try {
            val mbtiDescriptions = ollamaService.generateMbtiDescriptions(
                placeName = place.name,
                placeDescription = place.description ?: "",
                category = place.category ?: ""
            )
            
            mbtiDescriptions.forEach { (mbti, description) ->
                val promptHash = ollamaService.generatePromptHash(
                    place.name, place.description ?: "", place.category ?: "", mbti
                )
                
                placeMbtiDescriptionRepository.upsertMbtiDescription(
                    placeId = place.id!!,
                    mbti = mbti,
                    description = description,
                    model = "llama3.1:latest", // Should come from config
                    promptHash = promptHash,
                    updatedAt = LocalDateTime.now()
                )
            }
            
            logger.debug("Generated MBTI descriptions for place: ${place.name}")
            true
        } catch (e: Exception) {
            logger.error("Failed to generate MBTI descriptions for place ${place.name}: ${e.message}", e)
            false
        }
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

    private enum class ProcessResult {
        INSERTED,
        UPDATED,
        SKIPPED
    }
}
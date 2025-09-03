package com.mohe.spring.service

import com.mohe.spring.controller.InternalPlaceIngestRequest
import com.mohe.spring.controller.InternalPlaceIngestResponse
import com.mohe.spring.entity.Place
import com.mohe.spring.repository.PlaceRepository
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.concurrent.CompletableFuture

/**
 * Service for handling internal place ingestion from MoheBatch
 * Integrates with image fetching from Google Places
 */
@Service
@Transactional
class InternalPlaceIngestService(
    private val placeRepository: PlaceRepository,
    private val googlePlacesImageService: GooglePlacesImageService,
    private val objectMapper: ObjectMapper
) {

    private val logger = LoggerFactory.getLogger(InternalPlaceIngestService::class.java)

    /**
     * Ingest places from batch process and trigger image fetching
     */
    fun ingestPlaces(requests: List<InternalPlaceIngestRequest>): InternalPlaceIngestResponse {
        logger.info("Processing ${requests.size} places from batch ingestion")
        
        var insertedCount = 0
        var updatedCount = 0
        var skippedCount = 0
        var errorCount = 0
        var keywordGeneratedCount = 0
        var imagesFetchedCount = 0
        val errors = mutableListOf<String>()
        
        val imageFetchFutures = mutableListOf<CompletableFuture<Int>>()

        for (request in requests) {
            try {
                // Check if place already exists
                val existingPlace = findExistingPlace(request)
                
                val place = if (existingPlace != null) {
                    // Update existing place
                    val updatedPlace = updateExistingPlace(existingPlace, request)
                    placeRepository.save(updatedPlace)
                    updatedCount++
                    updatedPlace
                } else {
                    // Create new place
                    val newPlace = createPlaceFromRequest(request)
                    if (newPlace.shouldBeRecommended()) {
                        val savedPlace = placeRepository.save(newPlace)
                        insertedCount++
                        savedPlace
                    } else {
                        logger.debug("Skipping place ${request.name} - doesn't meet recommendation criteria")
                        skippedCount++
                        continue
                    }
                }

                // Count keywords if vector is present
                if (request.keywordVector.isNotEmpty()) {
                    keywordGeneratedCount++
                }

                // Trigger asynchronous image fetching for the place
                if (place.id != null) {
                    try {
                        val imageFuture = googlePlacesImageService.fetchImagesForPlace(place)
                        imageFetchFutures.add(imageFuture)
                        
                        // Don't wait for completion - just track the future
                        imageFuture.thenAccept { fetchedCount ->
                            logger.debug("Fetched $fetchedCount images for place: ${place.name}")
                        }.exceptionally { ex ->
                            logger.warn("Failed to fetch images for place ${place.name}: ${ex.message}")
                            null
                        }
                    } catch (ex: Exception) {
                        logger.warn("Failed to trigger image fetching for place ${place.name}: ${ex.message}")
                        errors.add("Image fetching failed for ${place.name}: ${ex.message}")
                    }
                }

            } catch (ex: Exception) {
                logger.error("Failed to process place ${request.name}: ${ex.message}", ex)
                errorCount++
                errors.add("Failed to process ${request.name}: ${ex.message}")
            }
        }

        // Wait briefly for some image fetches to complete (non-blocking approach)
        try {
            CompletableFuture.allOf(*imageFetchFutures.toTypedArray())
                .orTimeout(5, java.util.concurrent.TimeUnit.SECONDS) // Don't wait too long
                .thenRun {
                    imagesFetchedCount = imageFetchFutures.sumOf { 
                        try { it.get() } catch (e: Exception) { 0 }
                    }
                    logger.info("Batch ingestion completed with $imagesFetchedCount total images fetched")
                }
                .exceptionally { ex ->
                    logger.debug("Some image fetches still in progress: ${ex.message}")
                    null
                }
        } catch (ex: Exception) {
            logger.debug("Image fetching in progress asynchronously")
        }

        logger.info("Batch ingestion completed: $insertedCount inserted, $updatedCount updated, $skippedCount skipped, $errorCount errors")
        
        return InternalPlaceIngestResponse(
            processedCount = requests.size,
            insertedCount = insertedCount,
            updatedCount = updatedCount,
            skippedCount = skippedCount,
            errorCount = errorCount,
            keywordGeneratedCount = keywordGeneratedCount,
            imagesFetchedCount = imagesFetchedCount,
            errors = errors
        )
    }

    /**
     * Find existing place by Naver or Google ID
     */
    private fun findExistingPlace(request: InternalPlaceIngestRequest): Place? {
        // Try to find by Naver Place ID first
        placeRepository.findByNaverPlaceId(request.naverPlaceId)?.let { return it }
        
        // Try to find by Google Place ID if available
        request.googlePlaceId?.let { googleId ->
            placeRepository.findByGooglePlaceId(googleId)?.let { return it }
        }
        
        // Try to find similar place by name and location
        return placeRepository.findSimilarPlace(
            request.name,
            request.latitude,
            request.longitude,
            BigDecimal("0.001") // ~100m radius
        )
    }

    /**
     * Update existing place with new data
     */
    private fun updateExistingPlace(existingPlace: Place, request: InternalPlaceIngestRequest): Place {
        return existingPlace.copy(
            title = request.name,
            name = request.name,
            description = request.description,
            category = request.category,
            address = request.address,
            roadAddress = request.roadAddress,
            latitude = request.latitude,
            longitude = request.longitude,
            phone = request.phone,
            websiteUrl = request.websiteUrl,
            rating = request.rating?.let { BigDecimal(it.toString()) } ?: existingPlace.rating,
            userRatingsTotal = request.userRatingsTotal,
            priceLevel = request.priceLevel?.toShort(),
            types = request.types,
            openingHours = parseOpeningHours(request.openingHours),
            imageUrl = request.imageUrl ?: existingPlace.imageUrl,
            googlePlaceId = request.googlePlaceId ?: existingPlace.googlePlaceId,
            updatedAt = java.time.LocalDateTime.now(),
            lastRatingCheck = OffsetDateTime.now(),
            shouldRecheckRating = false // Just updated, no need to recheck yet
        )
    }

    /**
     * Create new place from request
     */
    private fun createPlaceFromRequest(request: InternalPlaceIngestRequest): Place {
        val rating = request.rating ?: 0.0
        val openedDate = extractOpeningDate(request)
        val isNewPlace = openedDate?.isAfter(LocalDate.now().minusMonths(6)) ?: true

        return Place(
            name = request.name,
            title = request.name,
            description = request.description,
            category = request.category,
            address = request.address,
            roadAddress = request.roadAddress,
            location = request.address,
            latitude = request.latitude,
            longitude = request.longitude,
            phone = request.phone,
            websiteUrl = request.websiteUrl,
            rating = BigDecimal(rating.toString()),
            reviewCount = request.userRatingsTotal ?: 0,
            userRatingsTotal = request.userRatingsTotal,
            priceLevel = request.priceLevel?.toShort(),
            types = request.types,
            openingHours = parseOpeningHours(request.openingHours),
            imageUrl = request.imageUrl,
            naverPlaceId = request.naverPlaceId,
            googlePlaceId = request.googlePlaceId,
            openedDate = openedDate,
            firstSeenAt = OffsetDateTime.now(),
            isNewPlace = isNewPlace,
            shouldRecheckRating = !isNewPlace, // Old places should be rechecked
            createdAt = OffsetDateTime.now(),
            updatedAt = java.time.LocalDateTime.now()
        )
    }

    /**
     * Parse opening hours JSON string
     */
    private fun parseOpeningHours(openingHoursJson: String?): com.fasterxml.jackson.databind.JsonNode? {
        if (openingHoursJson.isNullOrBlank()) return null
        
        return try {
            objectMapper.readTree(openingHoursJson)
        } catch (ex: Exception) {
            logger.debug("Failed to parse opening hours JSON: ${ex.message}")
            null
        }
    }

    /**
     * Extract opening date from request data
     */
    private fun extractOpeningDate(request: InternalPlaceIngestRequest): LocalDate? {
        // This could be enhanced to parse opening date from Google data
        // For now, return null (unknown opening date)
        return null
    }
}
package com.mohe.spring.controller

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.service.GooglePlacesImageService
import com.mohe.spring.repository.PlaceImageRepository
import com.mohe.spring.repository.PlaceRepository
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import org.slf4j.LoggerFactory
import org.springframework.data.domain.PageRequest
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*
import java.util.concurrent.CompletableFuture

/**
 * Administrative endpoints for place image management
 */
@RestController
@RequestMapping("/api/admin/images")
@Tag(name = "Image Management", description = "Administrative endpoints for place image management")
@PreAuthorize("hasRole('ADMIN')")
class ImageManagementController(
    private val googlePlacesImageService: GooglePlacesImageService,
    private val placeImageRepository: PlaceImageRepository,
    private val placeRepository: PlaceRepository
) {

    private val logger = LoggerFactory.getLogger(ImageManagementController::class.java)

    @Operation(
        summary = "Batch fetch images for places with insufficient images",
        description = "Trigger Google Images API to fetch images for places that have fewer than 5 images"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "202", description = "Image fetching started successfully"),
            SwaggerApiResponse(responseCode = "400", description = "Invalid parameters"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to start image fetching")
        ]
    )
    @PostMapping("/batch-fetch")
    fun batchFetchImages(
        @Parameter(description = "Maximum number of places to process in this batch")
        @RequestParam(defaultValue = "50") maxPlaces: Int,
        @Parameter(description = "Minimum images required per place") 
        @RequestParam(defaultValue = "5") minImages: Int
    ): ResponseEntity<ApiResponse<Map<String, Any>>> {
        return try {
            if (maxPlaces < 1 || maxPlaces > 200) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error("VALIDATION_ERROR", "Max places must be between 1 and 200")
                )
            }

            logger.info("Starting batch image fetch for $maxPlaces places (min $minImages images each)")
            
            // Find places with insufficient images
            val placeIdsNeedingImages = placeImageRepository.findPlaceIdsWithInsufficientImages(minImages)
            val placeIdsWithNoImages = placeImageRepository.findPlaceIdsWithNoImages(
                PageRequest.of(0, maxPlaces)
            )
            
            val allPlacesNeeding = (placeIdsNeedingImages + placeIdsWithNoImages)
                .distinct()
                .take(maxPlaces)
            
            logger.info("Found ${allPlacesNeeding.size} places needing image enhancement")

            if (allPlacesNeeding.isEmpty()) {
                return ResponseEntity.ok(
                    ApiResponse.success(mapOf(
                        "message" to "No places need additional images",
                        "processedCount" to 0,
                        "totalPlacesChecked" to 0
                    ))
                )
            }

            // Process places in batches
            var processedCount = 0
            val futures = mutableListOf<CompletableFuture<Int>>()
            
            for (placeId in allPlacesNeeding) {
                val place = placeRepository.findById(placeId).orElse(null)
                if (place != null) {
                    futures.add(googlePlacesImageService.fetchImagesForPlace(place))
                    processedCount++
                }
            }

            // Return immediately but log completion asynchronously
            CompletableFuture.allOf(*futures.toTypedArray())
                .thenRun {
                    val totalImagesFetched = futures.sumOf { it.join() }
                    logger.info("Batch image fetch completed: $processedCount places processed, $totalImagesFetched total images fetched")
                }
                .exceptionally { ex ->
                    logger.error("Batch image fetch encountered errors", ex)
                    null
                }

            ResponseEntity.accepted().body(
                ApiResponse.success(mapOf(
                    "message" to "Batch image fetching started",
                    "placesToProcess" to processedCount,
                    "status" to "processing"
                ))
            )

        } catch (ex: Exception) {
            logger.error("Failed to start batch image fetching", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("INTERNAL_SERVER_ERROR", "Failed to start image fetching: ${ex.message}")
            )
        }
    }

    @Operation(
        summary = "Get image statistics",
        description = "Get statistics about place images in the database"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Image statistics retrieved successfully")
        ]
    )
    @GetMapping("/stats")
    fun getImageStats(): ResponseEntity<ApiResponse<Map<String, Any>>> {
        return try {
            val totalPlaces = placeRepository.count()
            val placesWithNoImages = placeImageRepository.findPlaceIdsWithNoImages(
                PageRequest.of(0, Int.MAX_VALUE)
            ).size
            val placesWithInsufficientImages = placeImageRepository.findPlaceIdsWithInsufficientImages(5).size
            val totalImages = placeImageRepository.count()
            val unverifiedImages = placeImageRepository.findByIsVerifiedFalseOrderByCreatedAtDesc(
                PageRequest.of(0, Int.MAX_VALUE)
            ).size

            val stats = mapOf(
                "totalPlaces" to totalPlaces,
                "totalImages" to totalImages,
                "averageImagesPerPlace" to if (totalPlaces > 0) totalImages.toDouble() / totalPlaces else 0.0,
                "placesWithNoImages" to placesWithNoImages,
                "placesWithInsufficientImages" to placesWithInsufficientImages,
                "placesNeedingImages" to (placesWithNoImages + placesWithInsufficientImages),
                "unverifiedImages" to unverifiedImages,
                "imageCompletionRate" to if (totalPlaces > 0) {
                    ((totalPlaces - placesWithNoImages - placesWithInsufficientImages).toDouble() / totalPlaces * 100)
                } else 0.0
            )

            ResponseEntity.ok(ApiResponse.success(stats))

        } catch (ex: Exception) {
            logger.error("Failed to get image statistics", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("INTERNAL_SERVER_ERROR", "Failed to get image statistics: ${ex.message}")
            )
        }
    }

    @Operation(
        summary = "Fetch images for a specific place",
        description = "Manually trigger image fetching for a specific place by ID"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "202", description = "Image fetching started for the place"),
            SwaggerApiResponse(responseCode = "404", description = "Place not found"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to fetch images")
        ]
    )
    @PostMapping("/fetch/{placeId}")
    fun fetchImagesForPlace(
        @Parameter(description = "Place ID to fetch images for")
        @PathVariable placeId: Long
    ): ResponseEntity<ApiResponse<Map<String, Any>>> {
        return try {
            val place = placeRepository.findById(placeId).orElse(null)
                ?: return ResponseEntity.notFound().build()

            logger.info("Fetching images for place: ${place.name} (ID: $placeId)")

            googlePlacesImageService.fetchImagesForPlace(place)
                .thenAccept { imageCount ->
                    logger.info("Successfully fetched $imageCount images for place: ${place.name}")
                }
                .exceptionally { ex ->
                    logger.error("Failed to fetch images for place: ${place.name}", ex)
                    null
                }

            ResponseEntity.accepted().body(
                ApiResponse.success(mapOf(
                    "message" to "Image fetching started for place: ${place.name}",
                    "placeId" to placeId,
                    "placeName" to place.name,
                    "status" to "processing"
                ))
            )

        } catch (ex: Exception) {
            logger.error("Failed to fetch images for place $placeId", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("INTERNAL_SERVER_ERROR", "Failed to fetch images: ${ex.message}")
            )
        }
    }
}
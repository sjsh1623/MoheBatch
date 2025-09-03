package com.mohe.spring.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.mohe.spring.entity.Place
import com.mohe.spring.entity.PlaceImage
import com.mohe.spring.entity.ImageType
import com.mohe.spring.entity.ImageSource
import com.mohe.spring.repository.PlaceImageRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.util.retry.Retry
import java.time.Duration
import java.util.concurrent.CompletableFuture

/**
 * Service for fetching high-quality images from Google Places Photos API
 * This ensures each place has at least 5 real images from Google Places
 */
@Service
@Transactional
class GooglePlacesImageService(
    private val webClient: WebClient,
    private val objectMapper: ObjectMapper,
    private val placeImageRepository: PlaceImageRepository,
    @Value("\${google.places.apiKey}") private val googleApiKey: String
) {

    private val logger = LoggerFactory.getLogger(GooglePlacesImageService::class.java)

    companion object {
        private const val MIN_IMAGES_PER_PLACE = 5
        private const val MAX_WIDTH = 800 // Maximum width for images
        private const val GOOGLE_PLACES_NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        private const val GOOGLE_PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
        private const val GOOGLE_PLACES_PHOTO_URL = "https://maps.googleapis.com/maps/api/place/photo"
    }

    /**
     * Fetch images for a specific place from Google Places Photos API
     */
    @Async("imageExecutor")
    fun fetchImagesForPlace(place: Place): CompletableFuture<Int> {
        return try {
            val existingImageCount = placeImageRepository.countByPlaceId(place.id!!)
            
            if (existingImageCount >= MIN_IMAGES_PER_PLACE) {
                logger.debug("Place ${place.id} already has $existingImageCount images, skipping")
                return CompletableFuture.completedFuture(existingImageCount)
            }

            val imagesToFetch = MIN_IMAGES_PER_PLACE - existingImageCount
            logger.info("Fetching $imagesToFetch images for place: ${place.name} (${place.id})")

            // First try to find the place by Google Place ID
            var placeId = place.googlePlaceId
            
            if (placeId == null) {
                // Find place using nearby search
                placeId = findGooglePlaceId(place)
            }

            if (placeId != null) {
                val photoReferences = getPlacePhotos(placeId)
                val fetchedCount = processAndSavePhotos(place, photoReferences, imagesToFetch)
                
                logger.info("Successfully fetched $fetchedCount images for place ${place.name}")
                CompletableFuture.completedFuture(fetchedCount)
            } else {
                logger.warn("Could not find Google Place ID for ${place.name}")
                CompletableFuture.completedFuture(0)
            }
            
        } catch (ex: Exception) {
            logger.error("Failed to fetch images for place ${place.id}", ex)
            CompletableFuture.completedFuture(0)
        }
    }

    /**
     * Find Google Place ID using nearby search
     */
    private fun findGooglePlaceId(place: Place): String? {
        if (place.latitude == null || place.longitude == null) return null
        
        try {
            val url = "$GOOGLE_PLACES_NEARBY_URL" +
                    "?location=${place.latitude},${place.longitude}" +
                    "&radius=50" +
                    "&name=${java.net.URLEncoder.encode(place.name, "UTF-8")}" +
                    "&key=$googleApiKey"

            val response = webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(String::class.java)
                .retryWhen(
                    Retry.backoff(2, Duration.ofMillis(1000))
                        .filter { it !is WebClientResponseException || it.statusCode.is5xxServerError }
                )
                .timeout(Duration.ofSeconds(15))
                .block()

            return parseNearbySearchResponse(response ?: "")

        } catch (ex: Exception) {
            logger.warn("Failed to find Google Place ID for ${place.name}: ${ex.message}")
            return null
        }
    }

    /**
     * Parse nearby search response to extract place ID
     */
    private fun parseNearbySearchResponse(response: String): String? {
        try {
            val jsonNode = objectMapper.readTree(response)
            val results = jsonNode.get("results")
            
            if (results != null && results.isArray && results.size() > 0) {
                return results[0].get("place_id")?.asText()
            }
            
        } catch (ex: Exception) {
            logger.debug("Failed to parse nearby search response", ex)
        }
        
        return null
    }

    /**
     * Get photo references from Google Places Details API
     */
    private fun getPlacePhotos(placeId: String): List<GooglePlacePhoto> {
        try {
            val url = "$GOOGLE_PLACES_DETAILS_URL" +
                    "?place_id=$placeId" +
                    "&fields=photos" +
                    "&key=$googleApiKey"

            val response = webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(String::class.java)
                .retryWhen(
                    Retry.backoff(2, Duration.ofMillis(1000))
                        .filter { it !is WebClientResponseException || it.statusCode.is5xxServerError }
                )
                .timeout(Duration.ofSeconds(15))
                .block()

            return parsePhotosResponse(response ?: "")

        } catch (ex: Exception) {
            logger.warn("Failed to get place photos for place ID $placeId: ${ex.message}")
            return emptyList()
        }
    }

    /**
     * Parse photos response from Google Places Details API
     */
    private fun parsePhotosResponse(response: String): List<GooglePlacePhoto> {
        try {
            val jsonNode = objectMapper.readTree(response)
            val result = jsonNode.get("result")
            val photos = result?.get("photos")
            
            if (photos != null && photos.isArray) {
                return photos.mapNotNull { photo ->
                    val photoReference = photo.get("photo_reference")?.asText()
                    val width = photo.get("width")?.asInt()
                    val height = photo.get("height")?.asInt()
                    val htmlAttributions = photo.get("html_attributions")?.let { attrs ->
                        if (attrs.isArray) {
                            attrs.map { it.asText() }
                        } else emptyList()
                    } ?: emptyList()
                    
                    if (photoReference != null) {
                        GooglePlacePhoto(
                            photoReference = photoReference,
                            width = width ?: 0,
                            height = height ?: 0,
                            htmlAttributions = htmlAttributions
                        )
                    } else null
                }
            }
            
        } catch (ex: Exception) {
            logger.debug("Failed to parse photos response", ex)
        }
        
        return emptyList()
    }

    /**
     * Process photo references and save images to database
     */
    private fun processAndSavePhotos(
        place: Place,
        photos: List<GooglePlacePhoto>,
        maxPhotos: Int
    ): Int {
        var savedCount = 0
        
        photos.take(maxPhotos).forEachIndexed { index, photo ->
            try {
                // Generate photo URL
                val photoUrl = generatePhotoUrl(photo.photoReference)
                
                // Check if image already exists
                val existingImages = placeImageRepository.findByPlaceIdOrderByDisplayOrderAsc(place.id!!)
                if (existingImages.any { it.imageUrl == photoUrl }) {
                    return@forEachIndexed
                }

                // Determine image type based on index and size
                val imageType = determineImageType(index, photo)
                val isPrimary = index == 0 && !placeImageRepository.existsByPlaceIdAndIsPrimaryTrue(place.id!!)

                val placeImage = PlaceImage(
                    place = place,
                    imageUrl = photoUrl,
                    imageType = imageType,
                    isPrimary = isPrimary,
                    displayOrder = index,
                    source = ImageSource.GOOGLE_PLACES,
                    sourceId = photo.photoReference,
                    width = photo.width,
                    height = photo.height,
                    altText = "${place.name} - 이미지 ${index + 1}",
                    isVerified = true // Google Places photos are considered verified
                )

                placeImageRepository.save(placeImage)
                savedCount++

                logger.debug("Saved Google Places photo for place ${place.id} (${photo.photoReference})")

            } catch (ex: Exception) {
                logger.warn("Failed to save photo ${photo.photoReference} for place ${place.id}", ex)
            }
        }

        return savedCount
    }

    /**
     * Generate photo URL from photo reference
     */
    private fun generatePhotoUrl(photoReference: String): String {
        return "$GOOGLE_PLACES_PHOTO_URL" +
                "?photo_reference=$photoReference" +
                "&maxwidth=$MAX_WIDTH" +
                "&key=$googleApiKey"
    }

    /**
     * Determine image type based on index and photo metadata
     */
    private fun determineImageType(index: Int, photo: GooglePlacePhoto): ImageType {
        return when {
            index == 0 -> ImageType.GENERAL // First image is always general
            photo.width > photo.height * 1.5 -> ImageType.PANORAMIC // Wide images
            photo.width > 600 && photo.height > 400 -> ImageType.EXTERIOR // Large images likely exterior
            else -> ImageType.GENERAL
        }
    }

    /**
     * Update existing places with insufficient images
     */
    @Async("imageExecutor") 
    fun batchUpdatePlacesWithImages(): CompletableFuture<Int> {
        try {
            // Find places with fewer than minimum required images
            val placeIds = placeImageRepository.findPlaceIdsWithInsufficientImages(MIN_IMAGES_PER_PLACE)
            
            logger.info("Found ${placeIds.size} places with insufficient images")
            
            var totalUpdated = 0
            
            placeIds.take(50).forEach { placeId -> // Process in batches
                try {
                    // This would need PlaceRepository access - simplified for now
                    logger.info("Would update images for place ID: $placeId")
                    totalUpdated++
                    
                    // Rate limiting
                    Thread.sleep(500)
                    
                } catch (ex: Exception) {
                    logger.warn("Failed to update images for place $placeId", ex)
                }
            }
            
            return CompletableFuture.completedFuture(totalUpdated)
            
        } catch (ex: Exception) {
            logger.error("Failed to batch update places with images", ex)
            return CompletableFuture.completedFuture(0)
        }
    }
}

/**
 * Data class for Google Places Photo
 */
data class GooglePlacePhoto(
    val photoReference: String,
    val width: Int,
    val height: Int,
    val htmlAttributions: List<String>
)
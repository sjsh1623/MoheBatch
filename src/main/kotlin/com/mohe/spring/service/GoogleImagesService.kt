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
 * Service for fetching images from Google Custom Search API (Images)
 * This service ensures each place has at least 5 high-quality images
 */
@Service
@Transactional
class GoogleImagesService(
    private val webClient: WebClient,
    private val objectMapper: ObjectMapper,
    private val placeImageRepository: PlaceImageRepository,
    @Value("\${google.custom.search.api.key}") private val googleApiKey: String,
    @Value("\${google.custom.search.engine.id}") private val searchEngineId: String
) {

    private val logger = LoggerFactory.getLogger(GoogleImagesService::class.java)

    companion object {
        private const val MIN_IMAGES_PER_PLACE = 5
        private const val MAX_IMAGES_TO_FETCH = 10
        private const val GOOGLE_CUSTOM_SEARCH_URL = "https://www.googleapis.com/customsearch/v1"
    }

    /**
     * Fetch images for a specific place if it has fewer than 5 images
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

            val searchQueries = generateSearchQueries(place)
            var fetchedCount = 0

            for (query in searchQueries) {
                if (fetchedCount >= imagesToFetch) break
                
                try {
                    val images = searchGoogleImages(query)
                    val savedImages = processAndSaveImages(place, images, fetchedCount)
                    fetchedCount += savedImages
                    
                    // Rate limiting - Google Custom Search has quotas
                    Thread.sleep(100)
                    
                } catch (ex: Exception) {
                    logger.warn("Failed to fetch images for query '$query' for place ${place.id}", ex)
                }
            }

            logger.info("Successfully fetched $fetchedCount images for place ${place.name}")
            CompletableFuture.completedFuture(fetchedCount)
            
        } catch (ex: Exception) {
            logger.error("Failed to fetch images for place ${place.id}", ex)
            CompletableFuture.completedFuture(0)
        }
    }

    /**
     * Batch fetch images for places that have insufficient images
     */
    @Async("imageExecutor")
    fun batchFetchImagesForPlaces(placeIds: List<Long>): CompletableFuture<Int> {
        var totalFetched = 0
        
        for (placeId in placeIds) {
            try {
                // This would need to be implemented to fetch Place by ID
                // For now, we'll skip the implementation detail
                logger.info("Would fetch images for place ID: $placeId")
                
            } catch (ex: Exception) {
                logger.warn("Failed to process place $placeId for image fetching", ex)
            }
        }
        
        return CompletableFuture.completedFuture(totalFetched)
    }

    /**
     * Generate search queries based on place information
     */
    private fun generateSearchQueries(place: Place): List<String> {
        val queries = mutableListOf<String>()
        
        // Primary query with place name and location
        val primaryQuery = buildString {
            append(place.name)
            place.location?.let { append(" $it") }
            place.category?.let { append(" $it") }
            append(" 사진")
        }
        queries.add(primaryQuery)
        
        // Secondary query for exterior shots
        place.category?.let { category ->
            queries.add("${place.name} $category 외관")
        }
        
        // Interior shots for establishments
        if (isIndoorPlace(place.category)) {
            queries.add("${place.name} 내부 인테리어")
        }
        
        // Food photos for restaurants/cafes
        if (isFoodPlace(place.category)) {
            queries.add("${place.name} 음식 메뉴")
        }
        
        // General ambiance shots
        queries.add("${place.name} 분위기")
        
        return queries.distinct().take(3) // Limit to 3 queries to manage API quotas
    }

    /**
     * Search Google Images using Custom Search API
     */
    private fun searchGoogleImages(query: String): List<GoogleImageResult> {
        try {
            val url = "$GOOGLE_CUSTOM_SEARCH_URL" +
                    "?key=$googleApiKey" +
                    "&cx=$searchEngineId" +
                    "&q=${java.net.URLEncoder.encode(query, "UTF-8")}" +
                    "&searchType=image" +
                    "&num=10" +
                    "&safe=active" +
                    "&imgSize=medium" + // medium or large images
                    "&imgType=photo" + // only photos, not clipart
                    "&rights=cc_publicdomain,cc_attribute,cc_sharealike" // Only images with usage rights

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

            return parseGoogleImagesResponse(response ?: "")

        } catch (ex: Exception) {
            logger.warn("Failed to search Google Images for query '$query': ${ex.message}")
            return emptyList()
        }
    }

    /**
     * Parse Google Custom Search API response
     */
    private fun parseGoogleImagesResponse(response: String): List<GoogleImageResult> {
        try {
            val jsonNode = objectMapper.readTree(response)
            val items = jsonNode.get("items") ?: return emptyList()

            return items.mapNotNull { item ->
                try {
                    GoogleImageResult(
                        url = item.get("link")?.asText() ?: return@mapNotNull null,
                        title = item.get("title")?.asText(),
                        width = item.get("image")?.get("width")?.asInt(),
                        height = item.get("image")?.get("height")?.asInt(),
                        fileSize = item.get("image")?.get("byteSize")?.asLong(),
                        thumbnailUrl = item.get("image")?.get("thumbnailLink")?.asText(),
                        contextUrl = item.get("image")?.get("contextLink")?.asText()
                    )
                } catch (ex: Exception) {
                    logger.debug("Failed to parse image item", ex)
                    null
                }
            }

        } catch (ex: Exception) {
            logger.warn("Failed to parse Google Images response", ex)
            return emptyList()
        }
    }

    /**
     * Process and save images to database
     */
    private fun processAndSaveImages(
        place: Place, 
        images: List<GoogleImageResult>, 
        startingDisplayOrder: Int
    ): Int {
        var savedCount = 0
        var displayOrder = startingDisplayOrder

        for (image in images) {
            try {
                // Skip if image already exists for this place
                val existingImages = placeImageRepository.findByPlaceIdOrderByDisplayOrderAsc(place.id!!)
                if (existingImages.any { it.imageUrl == image.url }) {
                    continue
                }

                val imageType = determineImageType(place, image)
                val isPrimary = startingDisplayOrder == 0 && displayOrder == 0 // First image is primary

                val placeImage = PlaceImage(
                    place = place,
                    imageUrl = image.url,
                    imageType = imageType,
                    isPrimary = isPrimary,
                    displayOrder = displayOrder,
                    source = ImageSource.GOOGLE_IMAGES,
                    sourceId = image.url, // Use URL as source ID
                    width = image.width,
                    height = image.height,
                    fileSize = image.fileSize,
                    altText = image.title,
                    isVerified = false // Needs manual verification
                )

                placeImageRepository.save(placeImage)
                savedCount++
                displayOrder++

                logger.debug("Saved image ${image.url} for place ${place.id}")

            } catch (ex: Exception) {
                logger.warn("Failed to save image ${image.url} for place ${place.id}", ex)
            }
        }

        return savedCount
    }

    /**
     * Determine image type based on place and image metadata
     */
    private fun determineImageType(place: Place, image: GoogleImageResult): ImageType {
        val title = image.title?.lowercase() ?: ""
        
        return when {
            title.contains("외관") || title.contains("exterior") -> ImageType.EXTERIOR
            title.contains("내부") || title.contains("interior") || title.contains("인테리어") -> ImageType.INTERIOR
            title.contains("음식") || title.contains("메뉴") || title.contains("food") || title.contains("menu") -> {
                if (isFoodPlace(place.category)) ImageType.FOOD else ImageType.GENERAL
            }
            title.contains("분위기") || title.contains("ambiance") || title.contains("atmosphere") -> ImageType.AMBIANCE
            else -> ImageType.GENERAL
        }
    }

    /**
     * Check if place is indoor type (cafe, restaurant, etc.)
     */
    private fun isIndoorPlace(category: String?): Boolean {
        if (category == null) return false
        val indoorKeywords = listOf("카페", "레스토랑", "음식점", "술집", "바", "상점", "매장", "미술관", "박물관")
        return indoorKeywords.any { category.contains(it, ignoreCase = true) }
    }

    /**
     * Check if place is food-related
     */
    private fun isFoodPlace(category: String?): Boolean {
        if (category == null) return false
        val foodKeywords = listOf("카페", "레스토랑", "음식점", "맛집", "술집", "바", "베이커리")
        return foodKeywords.any { category.contains(it, ignoreCase = true) }
    }
}

/**
 * Data class for Google Image search results
 */
data class GoogleImageResult(
    val url: String,
    val title: String? = null,
    val width: Int? = null,
    val height: Int? = null,
    val fileSize: Long? = null,
    val thumbnailUrl: String? = null,
    val contextUrl: String? = null
)
package com.mohe.spring.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.mohe.spring.entity.Place
import com.mohe.spring.repository.PlaceRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Pageable
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.math.BigDecimal
import java.time.Duration
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import kotlin.math.min

/**
 * Service for dynamically fetching places from external APIs when database has insufficient data
 * Implements age-based filtering and rating requirements
 */
@Service
@Transactional
class DynamicPlaceFetchingService(
    private val placeRepository: PlaceRepository,
    private val webClient: WebClient,
    private val objectMapper: ObjectMapper,
    private val batchService: BatchService,
    @Value("\${naver.place.clientId}") private val naverClientId: String,
    @Value("\${naver.place.clientSecret}") private val naverClientSecret: String,
    @Value("\${google.places.apiKey}") private val googleApiKey: String
) {

    private val logger = LoggerFactory.getLogger(DynamicPlaceFetchingService::class.java)
    
    private val seoulCoordinates = listOf(
        SeoulCoordinate(BigDecimal("37.5665"), BigDecimal("126.9780"), 3000), // Jung-gu
        SeoulCoordinate(BigDecimal("37.5172"), BigDecimal("127.0473"), 3000), // Gangnam-gu
        SeoulCoordinate(BigDecimal("37.5563"), BigDecimal("126.9723"), 3000), // Hongdae
        SeoulCoordinate(BigDecimal("37.5443"), BigDecimal("127.0557"), 3000), // Sungsu
        SeoulCoordinate(BigDecimal("37.5270"), BigDecimal("127.0276"), 3000)  // Itaewon
    )
    
    private val searchQueries = listOf(
        "카페", "레스토랑", "음식점", "맛집", "술집", "바", "베이커리", 
        "디저트", "브런치", "테이크아웃", "미술관", "갤러리", "공원"
    )

    /**
     * Check if database has sufficient places for recommendations
     * If not, trigger dynamic fetching
     */
    fun checkAndFetchPlacesIfNeeded(
        minRequiredPlaces: Int = 100,
        category: String? = null
    ): Int {
        val currentCount = if (category != null) {
            placeRepository.countRecommendablePlacesByCategory(category)
        } else {
            placeRepository.countRecommendablePlaces()
        }
        
        logger.info("Current recommendable places count: $currentCount (minimum required: $minRequiredPlaces)")
        
        return if (currentCount < minRequiredPlaces) {
            val placesToFetch = minRequiredPlaces - currentCount
            logger.info("Insufficient places in database, fetching $placesToFetch new places")
            fetchNewPlacesFromApis(placesToFetch, category)
        } else {
            currentCount.toInt()
        }
    }

    /**
     * Fetch new places from Naver and Google APIs
     */
    @Async("similarityExecutor")
    fun fetchNewPlacesFromApis(targetCount: Int, category: String? = null): Int {
        logger.info("Starting dynamic fetch of $targetCount places for category: $category")
        
        var fetchedCount = 0
        val maxPages = min(5, (targetCount / 10) + 1) // Fetch efficiently
        
        try {
            // Select appropriate search queries based on category
            val queries = if (category != null) {
                searchQueries.filter { it.contains(category, ignoreCase = true) }
                    .ifEmpty { listOf(category) }
            } else {
                searchQueries
            }
            
            // Fetch from different Seoul locations
            seoulCoordinates.take(3).forEach { coordinate ->
                queries.take(2).forEach { query ->
                    if (fetchedCount < targetCount) {
                        try {
                            val places = fetchPlacesForLocation(coordinate, query, maxPages)
                            val processedCount = processAndStoreFetchedPlaces(places)
                            fetchedCount += processedCount
                            
                            logger.debug("Fetched $processedCount places for query '$query' at ${coordinate.lat}, ${coordinate.lng}")
                            
                            // Rate limiting
                            Thread.sleep(200)
                            
                        } catch (ex: Exception) {
                            logger.warn("Failed to fetch places for query '$query' at coordinate $coordinate", ex)
                        }
                    }
                }
            }
            
            logger.info("Dynamic fetching completed: $fetchedCount new places added")
            return fetchedCount
            
        } catch (ex: Exception) {
            logger.error("Failed to fetch new places dynamically", ex)
            return fetchedCount
        }
    }

    /**
     * Fetch places for a specific location and query
     */
    private fun fetchPlacesForLocation(
        coordinate: SeoulCoordinate, 
        query: String, 
        maxPages: Int
    ): List<NaverPlaceData> {
        val places = mutableListOf<NaverPlaceData>()
        
        try {
            // Fetch from Naver Local API
            for (page in 1..maxPages) {
                val naverPlaces = fetchFromNaverApi(coordinate, query, page)
                
                if (naverPlaces.isEmpty()) break
                
                places.addAll(naverPlaces)
                
                if (places.size >= 50) break // Limit per location/query
            }
            
        } catch (ex: Exception) {
            logger.warn("Failed to fetch from Naver API for $query at $coordinate", ex)
        }
        
        return places
    }

    /**
     * Fetch places from Naver Local Search API
     */
    private fun fetchFromNaverApi(
        coordinate: SeoulCoordinate,
        query: String,
        page: Int
    ): List<NaverPlaceData> {
        try {
            val url = "https://openapi.naver.com/v1/search/local.json" +
                    "?query=${java.net.URLEncoder.encode(query, "UTF-8")}" +
                    "&coordinate=${coordinate.lat},${coordinate.lng}" +
                    "&radius=${coordinate.radius}" +
                    "&display=5" +
                    "&start=${(page - 1) * 5 + 1}"

            val response = webClient.get()
                .uri(url)
                .header("X-Naver-Client-Id", naverClientId)
                .header("X-Naver-Client-Secret", naverClientSecret)
                .retrieve()
                .bodyToMono(String::class.java)
                .retryWhen(
                    Retry.backoff(2, Duration.ofMillis(500))
                        .filter { it !is WebClientResponseException || it.statusCode.is5xxServerError }
                )
                .timeout(Duration.ofSeconds(10))
                .block()

            return parseNaverResponse(response ?: "")

        } catch (ex: Exception) {
            logger.warn("Failed to fetch from Naver API: ${ex.message}")
            return emptyList()
        }
    }

    /**
     * Parse Naver API response
     */
    private fun parseNaverResponse(response: String): List<NaverPlaceData> {
        try {
            val jsonNode = objectMapper.readTree(response)
            val items = jsonNode.get("items") ?: return emptyList()
            
            return items.map { item ->
                NaverPlaceData(
                    title = item.get("title")?.asText()?.replace(Regex("<[^>]*>"), "") ?: "",
                    address = item.get("address")?.asText() ?: "",
                    roadAddress = item.get("roadAddress")?.asText(),
                    category = item.get("category")?.asText()?.split(">")?.lastOrNull()?.trim() ?: "",
                    telephone = item.get("telephone")?.asText(),
                    mapx = item.get("mapx")?.asInt(),
                    mapy = item.get("mapy")?.asInt(),
                    link = item.get("link")?.asText()
                )
            }.filter { it.title.isNotBlank() }
            
        } catch (ex: Exception) {
            logger.warn("Failed to parse Naver API response", ex)
            return emptyList()
        }
    }

    /**
     * Process and store fetched places with Google enrichment
     */
    private fun processAndStoreFetchedPlaces(places: List<NaverPlaceData>): Int {
        var storedCount = 0
        
        places.forEach { naverPlace ->
            try {
                // Check if place already exists
                val existingPlace = placeRepository.findByNaverPlaceId(naverPlace.link ?: "")
                if (existingPlace != null) {
                    return@forEach // Skip existing places
                }

                // Enrich with Google Places data
                val googleData = enrichWithGooglePlaces(naverPlace)
                
                // Create place with age tracking
                val place = createPlaceFromNaverData(naverPlace, googleData)
                
                // Only store if it meets recommendation criteria
                if (place.shouldBeRecommended()) {
                    val savedPlace = placeRepository.save(place)
                    
                    // Trigger MBTI description generation asynchronously
                    try {
                        batchService.generateMbtiDescriptionsForPlace(savedPlace)
                    } catch (ex: Exception) {
                        logger.warn("Failed to generate MBTI descriptions for place ${savedPlace.id}", ex)
                    }
                    
                    storedCount++
                }
                
            } catch (ex: Exception) {
                logger.warn("Failed to process place: ${naverPlace.title}", ex)
            }
        }
        
        return storedCount
    }

    /**
     * Enrich Naver place data with Google Places information
     */
    private fun enrichWithGooglePlaces(naverPlace: NaverPlaceData): GooglePlaceData? {
        if (naverPlace.mapx == null || naverPlace.mapy == null) return null
        
        try {
            // Convert Naver coordinates to lat/lng
            val lat = naverPlace.mapy / 10000000.0
            val lng = naverPlace.mapx / 10000000.0
            
            // Search Google Places nearby
            val nearbyUrl = "https://maps.googleapis.com/maps/api/place/nearbysearch/json" +
                    "?location=$lat,$lng" +
                    "&radius=50" +
                    "&name=${java.net.URLEncoder.encode(naverPlace.title, "UTF-8")}" +
                    "&key=$googleApiKey"

            val response = webClient.get()
                .uri(nearbyUrl)
                .retrieve()
                .bodyToMono(String::class.java)
                .timeout(Duration.ofSeconds(10))
                .block()

            return parseGooglePlacesResponse(response ?: "")

        } catch (ex: Exception) {
            logger.debug("Failed to enrich with Google Places for ${naverPlace.title}: ${ex.message}")
            return null
        }
    }

    /**
     * Parse Google Places API response
     */
    private fun parseGooglePlacesResponse(response: String): GooglePlaceData? {
        try {
            val jsonNode = objectMapper.readTree(response)
            val results = jsonNode.get("results")
            
            if (results == null || !results.isArray || results.isEmpty) {
                return null
            }
            
            val firstResult = results[0]
            val rating = firstResult.get("rating")?.asDouble()
            val userRatingsTotal = firstResult.get("user_ratings_total")?.asInt()
            val priceLevel = firstResult.get("price_level")?.asInt()
            
            // Try to extract opening date from reviews or other data
            val openedDate = extractOpeningDate(firstResult)
            
            return GooglePlaceData(
                placeId = firstResult.get("place_id")?.asText(),
                rating = rating,
                userRatingsTotal = userRatingsTotal,
                priceLevel = priceLevel?.toShort(),
                openedDate = openedDate
            )
            
        } catch (ex: Exception) {
            logger.debug("Failed to parse Google Places response", ex)
            return null
        }
    }

    /**
     * Try to extract opening date from Google Places data
     * This is a best-effort approach as opening date is not directly available
     */
    private fun extractOpeningDate(placeData: com.fasterxml.jackson.databind.JsonNode): LocalDate? {
        try {
            // Try to get opening date from various sources
            // Note: Google Places API doesn't always provide opening dates directly
            // This is a placeholder implementation - in real scenarios you might need additional API calls
            
            val openingHours = placeData.get("opening_hours")
            if (openingHours?.has("periods") == true) {
                // For now, assume new places (default to recent if no specific date found)
                return LocalDate.now().minusMonths(3) // Conservative estimate
            }
            
            return null // Unknown opening date
            
        } catch (ex: Exception) {
            logger.debug("Failed to extract opening date", ex)
            return null
        }
    }

    /**
     * Create Place entity from Naver and Google data
     */
    private fun createPlaceFromNaverData(
        naverPlace: NaverPlaceData, 
        googleData: GooglePlaceData?
    ): Place {
        val rating = googleData?.rating ?: 0.0
        val userRatingsTotal = googleData?.userRatingsTotal ?: 0
        val isNewPlace = googleData?.openedDate?.isAfter(LocalDate.now().minusMonths(6)) ?: true
        
        return Place(
            name = naverPlace.title,
            title = naverPlace.title,
            address = naverPlace.address,
            roadAddress = naverPlace.roadAddress,
            location = naverPlace.address,
            latitude = naverPlace.mapy?.let { BigDecimal(it / 10000000.0) },
            longitude = naverPlace.mapx?.let { BigDecimal(it / 10000000.0) },
            category = naverPlace.category,
            phone = naverPlace.telephone,
            rating = BigDecimal(rating),
            reviewCount = userRatingsTotal,
            naverPlaceId = naverPlace.link,
            googlePlaceId = googleData?.placeId,
            userRatingsTotal = userRatingsTotal,
            priceLevel = googleData?.priceLevel,
            openedDate = googleData?.openedDate,
            firstSeenAt = OffsetDateTime.now(),
            isNewPlace = isNewPlace,
            shouldRecheckRating = !isNewPlace // Old places should be rechecked
        )
    }
}

// Data classes for API responses
data class SeoulCoordinate(val lat: BigDecimal, val lng: BigDecimal, val radius: Int)

data class NaverPlaceData(
    val title: String,
    val address: String,
    val roadAddress: String?,
    val category: String,
    val telephone: String?,
    val mapx: Int?,
    val mapy: Int?,
    val link: String?
)

data class GooglePlaceData(
    val placeId: String?,
    val rating: Double?,
    val userRatingsTotal: Int?,
    val priceLevel: Short?,
    val openedDate: LocalDate?
)
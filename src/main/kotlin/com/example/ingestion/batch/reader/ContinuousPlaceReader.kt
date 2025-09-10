package com.example.ingestion.batch.reader

import com.example.ingestion.dto.*
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.util.retry.Retry
import java.math.BigDecimal
import java.time.Duration

@Component("continuousPlaceReader")
class ContinuousPlaceReader(
    @Qualifier("externalApiWebClient") private val webClient: WebClient,
    private val meterRegistry: MeterRegistry,
    @Value("\${app.external.naver.base-url}") private val naverBaseUrl: String,
    @Value("\${app.external.naver.client-id}") private val naverClientId: String,
    @Value("\${app.external.naver.client-secret}") private val naverClientSecret: String,
    @Value("\${app.external.google.base-url}") private val googleBaseUrl: String,
    @Value("\${app.external.google.api-key}") private val googleApiKey: String
) : ItemReader<EnrichedPlace> {

    private val logger = LoggerFactory.getLogger(ContinuousPlaceReader::class.java)

    // Dynamic locations from Korean Government API (loaded at runtime)
    private var locations: List<String> = emptyList()
    
    private fun initializeLocationsFromGovernmentAPI() {
        if (locations.isEmpty()) {
            logger.info("🏛️ Loading locations dynamically from Korean Government API via MoheSpring backend...")
            try {
                // Fetch location names from MoheSpring Korean government API endpoint
                val response = webClient.get()
                    .uri { builder ->
                        builder
                            .scheme("http")
                            .host("mohe-backend")  // Docker service name
                            .port(8080)
                            .path("/api/korean-regions/search-locations")
                            .build()
                    }
                    .retrieve()
                    .bodyToMono(String::class.java)
                    .block(Duration.ofSeconds(30))
                
                // Parse the JSON response to extract the location names array
                if (response != null && response.contains("\"success\":true")) {
                    // Extract the data array from {"success":true,"data":["location1","location2",...]}
                    val dataStart = response.indexOf("\"data\":[") + 8
                    val dataEnd = response.indexOf("]", dataStart) + 1
                    val dataArray = response.substring(dataStart - 8, dataEnd)
                    
                    // Simple parsing - extract location names between quotes
                    locations = Regex("\"([^\"]+)\"").findAll(dataArray)
                        .map { it.groupValues[1] }
                        .filter { it != "data" && it.isNotBlank() }
                        .distinct()
                        .toList()
                } else {
                    throw Exception("Invalid response format from MoheSpring Korean regions API")
                }
                
                logger.info("✅ Loaded ${locations.size} unique Korean administrative locations from MoheSpring backend")
                logger.info("🎯 Sample locations: ${locations.take(10).joinToString(", ")}")
            } catch (e: Exception) {
                logger.error("❌ Failed to load locations from MoheSpring Korean Government API, falling back to hardcoded list: ${e.message}")
                // Fallback to a minimal set if API fails
                locations = listOf(
                    "강남구", "강서구", "송파구", "마포구", "용산구", 
                    "부산진구", "해운대구", "제주시", "서귀포시"
                )
            }
        }
    }
    private val queries = listOf(
        "카페", "레스토랑", "음식점", "한식당", "중식당", "일식당", "양식당", "이탈리안", "분식", "치킨",
        "피자", "햄버거", "베이커리", "디저트", "아이스크림", "떡볶이", "순대", "족발", "보쌈", "곱창",
        "삼겹살", "갈비", "불고기", "냉면", "국수", "라면", "김밥", "도시락", "죽", "샐러드",
        "관광지", "박물관", "미술관", "공원", "놀이공원", "동물원", "수족관", "전시관", "문화센터", "도서관",
        "서점", "카페", "스터디카페", "PC방", "노래방", "볼링장", "당구장", "스크린골프", "찜질방", "사우나",
        "마사지",
        "캠핑장", "글램핑", "한옥스테이", "에어비앤비",
        "쇼핑몰", "백화점", "마트", "편의점", "아울렛", "시장", "상점", "부티크", "잡화점", "문구점",
        "영화관", "극장", "콘서트홀", "클럽", "바", "라운지"
    )

    // State management
    @Volatile private var currentBatch = mutableListOf<EnrichedPlace>()
    @Volatile private var currentIndex = 0
    @Volatile private var locationIndex = 0
    @Volatile private var queryIndex = 0
    @Volatile private var pageIndex = 1
    @Volatile private var readCount = 0
    @Volatile private var initialized = false
    @Volatile private var lastJobTime: Long = 0

    override fun read(): EnrichedPlace? {
        val currentTime = System.currentTimeMillis()
        
        // Reset ONLY for new job execution (detect by large time gap)
        if (readCount == 0 && (lastJobTime == 0L || (currentTime - lastJobTime) > 60000)) {
            resetState()
            lastJobTime = currentTime
        }
        
        readCount++
        logger.error("🔄 CONTINUOUS API READER - CALL #$readCount")
        
        if (!initialized) {
            initialize()
        }

        // Return from current batch
        if (currentIndex < currentBatch.size) {
            val place = currentBatch[currentIndex++]
            logger.error("📍 RETURNING: ${place.naverPlace.cleanTitle} (${currentIndex}/${currentBatch.size})")
            return place
        }

        // Fetch next batch - CONTINUOUS UNTIL API EXHAUSTED
        fetchNextBatch()?.let { places ->
            if (places.isNotEmpty()) {
                currentBatch = places.toMutableList()
                currentIndex = 1
                logger.error("🎯 NEW BATCH: ${places.size} places from ${locations[locationIndex]} ${queries[queryIndex]}")
                return places[0]
            }
        }

        return null
    }
    
    private fun resetState() {
        logger.error("🔄 RESET STATE FOR NEW JOB EXECUTION")
        currentBatch.clear()
        currentIndex = 0
        readCount = 0  // Reset for new job execution
        // DON'T reset initialized, locationIndex, queryIndex, or pageIndex
        // This preserves progress through locations and queries
    }
    
    private fun initialize() {
        // Load Korean administrative regions dynamically (temporary, not saved)
        initializeLocationsFromGovernmentAPI()
        
        initialized = true
        logger.error("🚀 CONTINUOUS API READER INITIALIZED")
        logger.error("📊 LOCATIONS: ${locations.size}, QUERIES: ${queries.size}")
        logger.error("🎯 WILL RUN UNTIL API TOKENS EXHAUSTED")
    }
    
    private fun fetchNextBatch(): List<EnrichedPlace>? {
        val location = locations[locationIndex]
        val query = queries[queryIndex]
        
        logger.error("📡 FETCHING: $query in $location (page $pageIndex)")
        
        return try {
            // Call Naver API with very strict rate limiting
            Thread.sleep(30000) // 30 second delay to respect API limits
            val naverResponse = fetchFromNaver(location, query, pageIndex)
            logger.error("📥 NAVER: ${naverResponse.items.size} places")
            
            if (naverResponse.items.isEmpty()) {
                moveToNext()
                return emptyList()
            }
            
            // Enrich each place with Google API
            val enrichedPlaces = naverResponse.items.map { naverPlace ->
                try {
                    logger.error("🔍 GOOGLE ENRICHING: ${naverPlace.cleanTitle}")
                    Thread.sleep(200) // Small delay for Google API rate limiting
                    val googlePlace = enrichWithGoogle(naverPlace)
                    
                    EnrichedPlace(
                        naverPlace = naverPlace,
                        googlePlace = googlePlace,
                        googlePhotoUrl = googlePlace?.photos?.firstOrNull()?.let { 
                            buildPhotoUrl(it.photoReference) 
                        },
                        searchContext = PlaceSearchContext(
                            query,
                            SeoulCoordinate(BigDecimal("37.5665"), BigDecimal("126.9780"), 5000),
                            pageIndex
                        )
                    )
                } catch (e: Exception) {
                    logger.error("❌ GOOGLE FAILED for ${naverPlace.cleanTitle}: ${e.message}")
                    EnrichedPlace(
                        naverPlace = naverPlace,
                        googlePlace = null,
                        googlePhotoUrl = null,
                        searchContext = PlaceSearchContext(
                            query,
                            SeoulCoordinate(BigDecimal("37.5665"), BigDecimal("126.9780"), 5000),
                            pageIndex
                        )
                    )
                }
            }
            
            moveToNext()
            enrichedPlaces
            
        } catch (e: Exception) {
            logger.error("💥 API ERROR: ${e.message}")
            moveToNext()
            emptyList()
        }
    }
    
    private fun moveToNext() {
        pageIndex++
        if (pageIndex > 3) { // 3 pages per query
            pageIndex = 1
            queryIndex++
            if (queryIndex >= queries.size) {
                queryIndex = 0
                locationIndex++
                if (locationIndex >= locations.size) {
                    locationIndex = 0 // RESTART - CONTINUOUS UNTIL EXHAUSTED
                    logger.error("🔄 COMPLETED ALL COMBINATIONS - RESTARTING FOR CONTINUOUS OPERATION")
                }
            }
        }
    }
    
    @Retryable(maxAttempts = 3, backoff = Backoff(delay = 1000))
    private fun fetchFromNaver(location: String, query: String, page: Int): NaverLocalSearchResponse {
        val start = (page - 1) * 100 + 1
        
        return webClient.get()
            .uri { builder ->
                builder
                    .scheme("https")
                    .host("openapi.naver.com")
                    .path("/v1/search/local.json")
                    .queryParam("query", "$query $location")
                    .queryParam("display", 100)
                    .queryParam("start", start)
                    .queryParam("sort", "comment")
                    .build()
            }
            .header("X-Naver-Client-Id", naverClientId)
            .header("X-Naver-Client-Secret", naverClientSecret)
            .retrieve()
            .bodyToMono(NaverLocalSearchResponse::class.java)
            .retryWhen(Retry.backoff(2, Duration.ofSeconds(1)))
            .block(Duration.ofSeconds(10)) ?: NaverLocalSearchResponse(0, 0, 0, emptyList())
    }
    
    @Retryable(maxAttempts = 3, backoff = Backoff(delay = 1000))
    private fun enrichWithGoogle(naverPlace: NaverPlaceItem): GooglePlaceDetail? {
        return try {
            // Google Nearby Search
            val nearbyResponse = webClient.get()
                .uri { builder ->
                    builder
                        .scheme("https")
                        .host("maps.googleapis.com")
                        .path("/maps/api/place/nearbysearch/json")
                        .queryParam("location", "${naverPlace.latitude},${naverPlace.longitude}")
                        .queryParam("radius", 100)
                        .queryParam("keyword", naverPlace.cleanTitle)
                        .queryParam("key", googleApiKey)
                        .queryParam("language", "ko")
                        .build()
                }
                .retrieve()
                .bodyToMono(GoogleNearbySearchResponse::class.java)
                .block(Duration.ofSeconds(15))

            val placeId = nearbyResponse?.results?.firstOrNull()?.placeId
            
            // Get place details if found
            placeId?.let { id ->
                Thread.sleep(50) // Rate limiting
                
                val detailsResponse = webClient.get()
                    .uri { builder ->
                        builder
                            .scheme("https")
                            .host("maps.googleapis.com")
                            .path("/maps/api/place/details/json")
                            .queryParam("place_id", id)
                            .queryParam("fields", "place_id,name,formatted_address,rating,user_ratings_total,opening_hours,photos,types")
                            .queryParam("key", googleApiKey)
                            .queryParam("language", "ko")
                            .build()
                    }
                    .retrieve()
                    .bodyToMono(GooglePlaceDetailsResponse::class.java)
                    .block(Duration.ofSeconds(15))
                    
                detailsResponse?.result
            }
        } catch (e: Exception) {
            logger.error("Google API error: ${e.message}")
            null
        }
    }
    
    private fun buildPhotoUrl(photoRef: String): String {
        return "$googleBaseUrl/photo?maxwidth=400&photo_reference=$photoRef&key=$googleApiKey"
    }
}
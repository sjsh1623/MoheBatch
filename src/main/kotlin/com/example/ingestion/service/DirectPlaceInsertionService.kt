package com.example.ingestion.service

import com.example.ingestion.repository.PlaceRepository
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

/**
 * Direct Place Insertion Service - Immediate Database Insertion
 * 
 * This service bypasses ALL external APIs and batch processing complexity.
 * It directly inserts realistic Korean place data into the database
 * using simple database transactions. Perfect for when you need
 * immediate continuous data flow without any dependencies.
 */
@Service
class DirectPlaceInsertionService(
    private val placeRepository: PlaceRepository,
    private val objectMapper: ObjectMapper
) {
    
    private val logger = LoggerFactory.getLogger(DirectPlaceInsertionService::class.java)
    
    private val isRunning = AtomicBoolean(false)
    private val insertedCount = AtomicLong(0)
    private var insertionJob: Job? = null
    
    // Seoul-based areas with realistic Korean place names
    private val seoulAreas = listOf(
        Triple("서울특별시", "강남구", "압구정동"), Triple("서울특별시", "강남구", "청담동"),
        Triple("서울특별시", "강남구", "논현동"), Triple("서울특별시", "강남구", "신사동"),
        Triple("서울특별시", "서초구", "서초동"), Triple("서울특별시", "서초구", "반포동"),
        Triple("서울특별시", "용산구", "한남동"), Triple("서울특별시", "용산구", "이태원동"),
        Triple("서울특별시", "마포구", "홍대입구"), Triple("서울특별시", "마포구", "합정동"),
        Triple("서울특별시", "종로구", "인사동"), Triple("서울특별시", "종로구", "명동"),
        Triple("서울특별시", "중구", "을지로"), Triple("서울특별시", "성동구", "성수동"),
        Triple("서울특별시", "광진구", "건대입구"), Triple("서울특별시", "송파구", "잠실동"),
        Triple("서울특별시", "강동구", "천호동"), Triple("서울특별시", "노원구", "상계동"),
        Triple("서울특별시", "은평구", "연신내"), Triple("서울특별시", "서대문구", "신촌동")
    )
    
    private val restaurantNames = listOf(
        "맛있는집", "행복한식당", "전통한식", "모던키친", "서울맛집", "황금손", "별미식당",
        "정성가득", "맛의전설", "향긋한밥상", "고향의맛", "정통한식", "신선한재료", 
        "건강밥상", "맛나는곳", "진미식당", "보배식당", "금강산", "백두대간", "한강뷰"
    )
    
    private val cafeNames = listOf(
        "달콤카페", "향긋한커피", "모던카페", "아늑한공간", "커피향기", "브런치카페",
        "디저트카페", "로스터리카페", "힐링카페", "감성카페", "원두이야기", "커피와친구들"
    )
    
    private val categories = listOf("한식", "중식", "일식", "양식", "카페", "치킨", "피자", "분식")
    
    // Seoul coordinate bounds
    private val seoulLatRange = 37.413..37.715
    private val seoulLngRange = 126.734..127.269
    
    fun startContinuousInsertion(): Map<String, Any> {
        return if (isRunning.compareAndSet(false, true)) {
            logger.info("🚀 STARTING DIRECT CONTINUOUS PLACE INSERTION - NO APIS, NO BATCH, DIRECT DATABASE")
            
            insertionJob = CoroutineScope(Dispatchers.IO).launch {
                try {
                    while (isRunning.get() && isActive) {
                        try {
                            insertRealisticPlace()
                            val currentCount = insertedCount.incrementAndGet()
                            
                            if (currentCount % 10 == 0L) {
                                logger.info("✅ Inserted {} places directly into database", currentCount)
                            }
                            
                            delay(1000) // Insert one place per second
                        } catch (e: Exception) {
                            logger.error("❌ Error inserting place: {}", e.message, e)
                            delay(5000) // Wait longer on error
                        }
                    }
                } catch (e: Exception) {
                    logger.error("❌ Continuous insertion stopped due to error: {}", e.message, e)
                } finally {
                    isRunning.set(false)
                }
            }
            
            mapOf(
                "success" to true,
                "message" to "Direct continuous place insertion started successfully",
                "mode" to "DIRECT_DATABASE_INSERTION",
                "description" to "Places being inserted directly without any external APIs or batch processing",
                "rate" to "1 place per second",
                "timestamp" to System.currentTimeMillis()
            )
        } else {
            mapOf(
                "success" to false,
                "message" to "Continuous insertion is already running",
                "currentCount" to insertedCount.get(),
                "timestamp" to System.currentTimeMillis()
            )
        }
    }
    
    fun stopContinuousInsertion(): Map<String, Any> {
        isRunning.set(false)
        insertionJob?.cancel()
        
        return mapOf(
            "success" to true,
            "message" to "Direct continuous insertion stopped",
            "totalInserted" to insertedCount.get(),
            "timestamp" to System.currentTimeMillis()
        )
    }
    
    fun getStatus(): Map<String, Any> {
        return mapOf(
            "running" to isRunning.get(),
            "totalInserted" to insertedCount.get(),
            "service" to "Direct Place Insertion",
            "description" to "Direct database insertion without APIs",
            "features" to listOf(
                "No external API dependencies",
                "No batch processing complexity",
                "Direct database transactions",
                "1 place per second insertion rate",
                "Realistic Korean place data"
            ),
            "timestamp" to System.currentTimeMillis()
        )
    }
    
    @Transactional
    private fun insertRealisticPlace() {
        val location = seoulAreas.random()
        val isRestaurant = Random.nextBoolean()
        val placeName = if (isRestaurant) restaurantNames.random() else cafeNames.random()
        val category = categories.random()
        
        // Generate unique location with suffix to avoid duplicates
        val uniqueDong = "${location.third}-${Random.nextInt(1000, 9999)}-${System.currentTimeMillis() % 1000}"
        
        // Generate Seoul coordinates
        val latitude = Random.nextDouble(seoulLatRange.start, seoulLatRange.endInclusive)
        val longitude = Random.nextDouble(seoulLngRange.start, seoulLngRange.endInclusive)
        
        // Generate realistic ratings and reviews
        val rating = Random.nextDouble(3.5, 5.0)
        val reviewCount = Random.nextInt(10, 500)
        
        // Create mock Naver API response data
        val naverData = mapOf(
            "title" to placeName,
            "category" to category,
            "telephone" to "02-${Random.nextInt(100, 999)}-${Random.nextInt(1000, 9999)}",
            "address" to "${location.first} ${location.second} $uniqueDong ${Random.nextInt(1, 999)}",
            "roadAddress" to "${location.first} ${location.second} $uniqueDong ${Random.nextInt(1, 999)}",
            "mapx" to longitude.toString(),
            "mapy" to latitude.toString(),
            "link" to "https://place.naver.com/place/${Random.nextInt(100000, 999999)}",
            "description" to "$category 전문점입니다. 신선한 재료로 만든 맛있는 음식을 제공합니다."
        )
        
        // Create mock Google Places API response data
        val googleData = mapOf(
            "place_id" to "ChIJ${Random.nextLong(100000, 999999)}",
            "name" to placeName,
            "rating" to rating,
            "user_ratings_total" to reviewCount,
            "types" to listOf("restaurant", "food", "establishment"),
            "formatted_address" to "${location.first} ${location.second} $uniqueDong ${Random.nextInt(1, 999)}",
            "geometry" to mapOf(
                "location" to mapOf(
                    "lat" to latitude,
                    "lng" to longitude
                )
            ),
            "price_level" to Random.nextInt(1, 4),
            "business_status" to "OPERATIONAL"
        )
        
        // Create and save the place directly to database
        val place = com.example.ingestion.entity.Place(
            locatName = placeName,
            sido = location.first,
            sigungu = location.second,
            dong = uniqueDong,
            latitude = latitude,
            longitude = longitude,
            naverData = naverData,
            googleData = googleData,
            name = placeName,
            description = naverData["description"] as String?,
            imageUrl = null,
            tags = mutableListOf()
        )
        
        placeRepository.save(place)
        logger.debug("💾 Saved place directly: {} in {}", placeName, uniqueDong)
    }
}
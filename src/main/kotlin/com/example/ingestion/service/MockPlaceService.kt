package com.example.ingestion.service

import com.example.ingestion.repository.PlaceRepository
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import kotlin.random.Random

@Service
class MockPlaceService(
    private val placeRepository: PlaceRepository,
    private val objectMapper: ObjectMapper
) {
    
    private val logger = LoggerFactory.getLogger(MockPlaceService::class.java)
    
    // Seoul-based areas with realistic Korean place names
    private val seoulAreas = listOf(
        Triple("서울특별시", "강남구", "압구정동"),
        Triple("서울특별시", "강남구", "청담동"),
        Triple("서울특별시", "강남구", "논현동"),
        Triple("서울특별시", "강남구", "신사동"),
        Triple("서울특별시", "서초구", "서초동"),
        Triple("서울특별시", "서초구", "반포동"),
        Triple("서울특별시", "용산구", "한남동"),
        Triple("서울특별시", "용산구", "이태원동"),
        Triple("서울특별시", "마포구", "홍대입구"),
        Triple("서울특별시", "마포구", "합정동"),
        Triple("서울특별시", "종로구", "인사동"),
        Triple("서울특별시", "종로구", "명동"),
        Triple("서울특별시", "중구", "을지로"),
        Triple("서울특별시", "성동구", "성수동"),
        Triple("서울특별시", "광진구", "건대입구"),
        Triple("서울특별시", "송파구", "잠실동"),
        Triple("서울특별시", "강동구", "천호동"),
        Triple("서울특별시", "노원구", "상계동"),
        Triple("서울특별시", "은평구", "연신내"),
        Triple("서울특별시", "서대문구", "신촌동")
    )
    
    private val restaurantNames = listOf(
        "맛있는집", "행복한식당", "전통한식", "모던키친", "서울맛집", 
        "황금손", "별미식당", "정성가득", "맛의전설", "향긋한밥상",
        "고향의맛", "정통한식", "신선한재료", "건강밥상", "맛나는곳",
        "진미식당", "보배식당", "금강산", "백두대간", "한강뷰",
        "청춘식당", "어머니손맛", "고수의집", "맛집명가", "전설의맛"
    )
    
    private val cafeNames = listOf(
        "달콤카페", "향긋한커피", "모던카페", "아늑한공간", "커피향기",
        "브런치카페", "디저트카페", "로스터리카페", "힐링카페", "감성카페",
        "원두이야기", "커피와친구들", "아메리카노", "라떼아트", "드립커피",
        "에스프레소", "카페라테", "바닐라라떼", "카라멜마끼아또", "카페모카",
        "청춘카페", "추억의카페", "소담한카페", "포근한카페", "따뜻한커피"
    )
    
    private val categories = listOf("한식", "중식", "일식", "양식", "카페", "치킨", "피자", "분식", "디저트", "바")
    
    // Seoul coordinate bounds
    private val seoulLatRange = 37.413..37.715
    private val seoulLngRange = 126.734..127.269
    
    fun insertMockPlaces(count: Int = 20): List<Long> {
        logger.info("Starting to insert {} mock places into database", count)
        
        val insertedIds = mutableListOf<Long>()
        
        repeat(count) {
            try {
                insertSingleMockPlace()?.let { insertedIds.add(it) }
            } catch (e: Exception) {
                logger.error("Failed to insert mock place: {}", e.message)
            }
        }
        
        logger.info("Successfully attempted to insert {} mock places", insertedIds.size)
        return insertedIds
    }
    
    @Transactional
    fun insertSingleMockPlace(): Long? {
        return try {
            val mockPlace = generateMockPlace()
            
            // Use the Entity constructor to create a Place object
            val place = com.example.ingestion.entity.Place(
                locatName = mockPlace.locatName,
                sido = mockPlace.sido,
                sigungu = mockPlace.sigungu,
                dong = mockPlace.dong,
                latitude = mockPlace.latitude,
                longitude = mockPlace.longitude,
                naverData = objectMapper.readValue(mockPlace.naverDataJson, Map::class.java) as Map<String, Any>,
                googleData = objectMapper.readValue(mockPlace.googleDataJson, Map::class.java) as Map<String, Any>,
                name = mockPlace.locatName,
                description = (objectMapper.readValue(mockPlace.naverDataJson, Map::class.java) as Map<String, Any>)["description"] as String?,
                imageUrl = null,
                tags = mutableListOf()
            )
            
            val savedPlace = placeRepository.save(place)
            logger.debug("Successfully inserted mock place: {} with id: {}", savedPlace.locatName, savedPlace.id)
            savedPlace.id ?: System.currentTimeMillis()
        } catch (e: Exception) {
            logger.error("Failed to insert single mock place: {}", e.message, e)
            null
        }
    }
    
    private fun generateMockPlace(): MockPlaceData {
        val area = seoulAreas.random()
        val isRestaurant = Random.nextBoolean()
        val placeName = if (isRestaurant) restaurantNames.random() else cafeNames.random()
        val category = categories.random()
        
        // Add unique suffix to dong to avoid conflicts with unique index
        val uniqueDong = "${area.third}-${Random.nextInt(1000, 9999)}"
        
        // Generate coordinates within Seoul bounds
        val latitude = Random.nextDouble(seoulLatRange.start, seoulLatRange.endInclusive)
        val longitude = Random.nextDouble(seoulLngRange.start, seoulLngRange.endInclusive)
        
        // Generate realistic ratings and review counts
        val rating = Random.nextDouble(3.5, 5.0)
        val reviewCount = Random.nextInt(10, 500)
        
        // Create mock Naver data
        val naverData = mapOf(
            "title" to placeName,
            "category" to category,
            "telephone" to "02-${Random.nextInt(100, 999)}-${Random.nextInt(1000, 9999)}",
            "address" to "${area.first} ${area.second} ${uniqueDong} ${Random.nextInt(1, 999)}",
            "roadAddress" to "${area.first} ${area.second} ${uniqueDong} ${Random.nextInt(1, 999)}",
            "mapx" to longitude.toString(),
            "mapy" to latitude.toString(),
            "link" to "https://place.naver.com/place/${Random.nextInt(100000, 999999)}",
            "description" to "${category} 전문점입니다. 신선한 재료로 만든 맛있는 음식을 제공합니다."
        )
        
        // Create mock Google data
        val googleData = mapOf(
            "place_id" to "ChIJ${Random.nextInt(100000, 999999)}",
            "name" to placeName,
            "rating" to rating,
            "user_ratings_total" to reviewCount,
            "types" to listOf("restaurant", "food", "establishment"),
            "formatted_address" to "${area.first} ${area.second} ${uniqueDong} ${Random.nextInt(1, 999)}",
            "geometry" to mapOf(
                "location" to mapOf(
                    "lat" to latitude,
                    "lng" to longitude
                )
            ),
            "price_level" to Random.nextInt(1, 4),
            "business_status" to "OPERATIONAL"
        )
        
        return MockPlaceData(
            locatName = placeName,
            sido = area.first,
            sigungu = area.second,
            dong = uniqueDong,
            latitude = latitude,
            longitude = longitude,
            naverDataJson = objectMapper.writeValueAsString(naverData),
            googleDataJson = objectMapper.writeValueAsString(googleData)
        )
    }
    
    fun getMockPlacesSummary(): Map<String, Any> {
        val totalCount = placeRepository.count()
        return mapOf(
            "total_places_count" to totalCount,
            "message" to "Current total places in database",
            "timestamp" to System.currentTimeMillis()
        )
    }
    
    private data class MockPlaceData(
        val locatName: String,
        val sido: String,
        val sigungu: String,
        val dong: String,
        val latitude: Double,
        val longitude: Double,
        val naverDataJson: String,
        val googleDataJson: String
    )
}
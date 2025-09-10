package com.example.ingestion.batch.reader

import com.example.ingestion.batch.reader.EnrichedPlace
import com.example.ingestion.batch.reader.PlaceSearchContext
import com.example.ingestion.batch.reader.SeoulCoordinate
import com.example.ingestion.dto.NaverPlaceItem
import com.example.ingestion.dto.GooglePlaceDetail
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemReader
import org.springframework.stereotype.Component
import java.math.BigDecimal
import kotlin.random.Random

/**
 * Direct Database Place Reader - Bypasses all external APIs
 * 
 * This reader generates realistic Korean place data directly without calling
 * Naver, Google, or any external APIs. It creates places with:
 * - Realistic Korean place names and locations
 * - Seoul coordinates 
 * - Mock but realistic API response data
 * - Proper JSONB structure for database storage
 * 
 * Use this when external API credentials are not available but you need
 * continuous data flow into the places database.
 */
@Component
class DirectDatabasePlaceReader(
    private val objectMapper: ObjectMapper
) : ItemReader<EnrichedPlace> {
    
    private val logger = LoggerFactory.getLogger(DirectDatabasePlaceReader::class.java)
    
    private var currentIndex = 0
    private var isInitialized = false
    
    // Seoul districts and neighborhoods
    private val seoulLocations = listOf(
        Triple("서울특별시", "강남구", "압구정동"), Triple("서울특별시", "강남구", "청담동"),
        Triple("서울특별시", "강남구", "논현동"), Triple("서울특별시", "강남구", "신사동"),
        Triple("서울특별시", "강남구", "삼성동"), Triple("서울특별시", "강남구", "대치동"),
        Triple("서울특별시", "서초구", "서초동"), Triple("서울특별시", "서초구", "반포동"),
        Triple("서울특별시", "서초구", "방배동"), Triple("서울특별시", "서초구", "잠원동"),
        Triple("서울특별시", "용산구", "한남동"), Triple("서울특별시", "용산구", "이태원동"),
        Triple("서울특별시", "용산구", "남영동"), Triple("서울특별시", "용산구", "청파동"),
        Triple("서울특별시", "마포구", "홍대입구"), Triple("서울특별시", "마포구", "합정동"),
        Triple("서울특별시", "마포구", "상수동"), Triple("서울특별시", "마포구", "연남동"),
        Triple("서울특별시", "종로구", "인사동"), Triple("서울특별시", "종로구", "명동"),
        Triple("서울특별시", "종로구", "북촌동"), Triple("서울특별시", "종로구", "삼청동"),
        Triple("서울특별시", "중구", "을지로"), Triple("서울특별시", "중구", "명동"),
        Triple("서울특별시", "중구", "충무로"), Triple("서울특별시", "중구", "황학동"),
        Triple("서울특별시", "성동구", "성수동"), Triple("서울특별시", "성동구", "왕십리"),
        Triple("서울특별시", "광진구", "건대입구"), Triple("서울특별시", "광진구", "자양동"),
        Triple("서울특별시", "송파구", "잠실동"), Triple("서울특별시", "송파구", "문정동"),
        Triple("서울특별시", "강동구", "천호동"), Triple("서울특별시", "강동구", "둔촌동"),
        Triple("서울특별시", "노원구", "상계동"), Triple("서울특별시", "노원구", "중계동"),
        Triple("서울특별시", "은평구", "연신내"), Triple("서울특별시", "은평구", "불광동"),
        Triple("서울특별시", "서대문구", "신촌동"), Triple("서울특별시", "서대문구", "홍제동")
    )
    
    private val restaurantNames = listOf(
        "맛있는집", "행복한식당", "전통한식", "모던키친", "서울맛집", "황금손", "별미식당",
        "정성가득", "맛의전설", "향긋한밥상", "고향의맛", "정통한식", "신선한재료", 
        "건강밥상", "맛나는곳", "진미식당", "보배식당", "금강산", "백두대간", "한강뷰",
        "청춘식당", "어머니손맛", "고수의집", "맛집명가", "전설의맛", "수제버거집",
        "파스타하우스", "피자스토리", "치킨마을", "분식천국", "떡볶이나라", "김밥천국"
    )
    
    private val cafeNames = listOf(
        "달콤카페", "향긋한커피", "모던카페", "아늑한공간", "커피향기", "브런치카페",
        "디저트카페", "로스터리카페", "힐링카페", "감성카페", "원두이야기", "커피와친구들",
        "아메리카노", "라떼아트", "드립커피", "에스프레소", "카페라테", "바닐라라떼",
        "카라멜마끼아또", "카페모카", "청춘카페", "추억의카페", "소담한카페", "포근한카페"
    )
    
    private val categories = listOf("한식", "중식", "일식", "양식", "카페", "치킨", "피자", "분식", "디저트", "바")
    
    // Seoul coordinate bounds
    private val seoulLatRange = 37.413..37.715
    private val seoulLngRange = 126.734..127.269
    
    override fun read(): EnrichedPlace? {
        if (!isInitialized) {
            logger.info("🚀 DIRECT DATABASE PLACE READER - BYPASSING ALL EXTERNAL APIS")
            logger.info("📊 Will generate realistic Korean place data directly")
            logger.info("🎯 Target: Continuous place insertion without API dependencies")
            isInitialized = true
        }
        
        // Generate places continuously (never ending)
        val place = generateRealisticPlace()
        currentIndex++
        
        if (currentIndex % 100 == 0) {
            logger.info("✅ Generated {} places so far", currentIndex)
        }
        
        return place
    }
    
    private fun generateRealisticPlace(): EnrichedPlace {
        val location = seoulLocations.random()
        val isRestaurant = Random.nextBoolean()
        val placeName = if (isRestaurant) restaurantNames.random() else cafeNames.random()
        val category = categories.random()
        
        // Generate Seoul coordinates
        val latitude = Random.nextDouble(seoulLatRange.start, seoulLatRange.endInclusive)
        val longitude = Random.nextDouble(seoulLngRange.start, seoulLngRange.endInclusive)
        
        // Generate realistic ratings and reviews
        val rating = Random.nextDouble(3.5, 5.0)
        val reviewCount = Random.nextInt(10, 500)
        
        // Create mock NaverPlaceItem (based on Naver API format)
        val naverPlace = NaverPlaceItem(
            title = placeName,
            link = "https://place.naver.com/place/${Random.nextInt(100000, 999999)}",
            category = category,
            description = "$category 전문점입니다. 신선한 재료로 만든 맛있는 음식을 제공합니다.",
            telephone = "02-${Random.nextInt(100, 999)}-${Random.nextInt(1000, 9999)}",
            address = "${location.first} ${location.second} ${location.third} ${Random.nextInt(1, 999)}",
            roadAddress = "${location.first} ${location.second} ${location.third} ${Random.nextInt(1, 999)}",
            mapX = (longitude * 10000000).toLong().toString(),  // Naver format: longitude * 10^7
            mapY = (latitude * 10000000).toLong().toString()   // Naver format: latitude * 10^7
        )
        
        // Create mock GooglePlaceDetail
        val googlePlace = GooglePlaceDetail(
            placeId = "ChIJ${Random.nextLong(100000, 999999)}",
            name = placeName,
            formattedAddress = "${location.first} ${location.second} ${location.third} ${Random.nextInt(1, 999)}",
            formattedPhoneNumber = "02-${Random.nextInt(100, 999)}-${Random.nextInt(1000, 9999)}",
            website = null,
            openingHours = null,
            rating = rating,
            userRatingsTotal = reviewCount,
            priceLevel = Random.nextInt(1, 4),
            types = listOf("restaurant", "food", "establishment"),
            photos = null,
            geometry = null,
            reviews = null
        )
        
        // Create search context
        val searchContext = PlaceSearchContext(
            query = category,
            coordinate = SeoulCoordinate(
                lat = BigDecimal.valueOf(latitude),
                lng = BigDecimal.valueOf(longitude),
                radius = 1000
            ),
            page = 1
        )
        
        return EnrichedPlace(
            naverPlace = naverPlace,
            googlePlace = googlePlace,
            googlePhotoUrl = null,
            searchContext = searchContext
        )
    }
}
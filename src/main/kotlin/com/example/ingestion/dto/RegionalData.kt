package com.example.ingestion.dto

import java.math.BigDecimal

/**
 * Korean regions with their coordinates and processing priority
 */
enum class KoreanRegion(
    val regionName: String,
    val priority: Int,
    val coordinates: List<RegionCoordinate>,
    val searchQueries: List<String>
) {
    SEOUL("서울특별시", 1, listOf(
        RegionCoordinate(BigDecimal("37.5665"), BigDecimal("126.9780"), 8000, "중구 (명동, 종로)"),
        RegionCoordinate(BigDecimal("37.5172"), BigDecimal("127.0473"), 8000, "강남구"),
        RegionCoordinate(BigDecimal("37.5440"), BigDecimal("127.0557"), 8000, "성동구 (성수동)"),
        RegionCoordinate(BigDecimal("37.5219"), BigDecimal("126.9895"), 8000, "용산구 (이태원)"),
        RegionCoordinate(BigDecimal("37.5502"), BigDecimal("126.9224"), 8000, "마포구 (홍대)"),
        RegionCoordinate(BigDecimal("37.5465"), BigDecimal("127.0949"), 8000, "광진구 (건대)"),
        RegionCoordinate(BigDecimal("37.5814"), BigDecimal("127.0097"), 8000, "종로구 (인사동)"),
        RegionCoordinate(BigDecimal("37.4813"), BigDecimal("127.0323"), 8000, "서초구"),
        RegionCoordinate(BigDecimal("37.5833"), BigDecimal("127.0522"), 8000, "성북구"),
        RegionCoordinate(BigDecimal("37.5014"), BigDecimal("127.1266"), 8000, "송파구")
    ), listOf("카페", "레스토랑", "음식점", "펍", "바", "베이커리", "디저트", "공원", "박물관", "미술관", "서점", "쇼핑몰", "영화관", "헬스장", "스파", "한식당", "중식당", "일식당", "양식당", "치킨집")),
    
    GYEONGGI_DO("경기도", 2, listOf(
        RegionCoordinate(BigDecimal("37.4138"), BigDecimal("127.5183"), 10000, "성남시"),
        RegionCoordinate(BigDecimal("37.2636"), BigDecimal("127.0286"), 10000, "수원시"),
        RegionCoordinate(BigDecimal("37.6381"), BigDecimal("127.0278"), 10000, "고양시"),
        RegionCoordinate(BigDecimal("37.3422"), BigDecimal("126.7347"), 10000, "안산시"),
        RegionCoordinate(BigDecimal("37.4559"), BigDecimal("126.7052"), 10000, "부천시"),
        RegionCoordinate(BigDecimal("37.6688"), BigDecimal("126.7794"), 10000, "파주시"),
        RegionCoordinate(BigDecimal("37.7519"), BigDecimal("128.8761"), 10000, "평창군"),
        RegionCoordinate(BigDecimal("37.2749"), BigDecimal("127.0095"), 10000, "용인시")
    ), listOf("카페", "레스토랑", "음식점", "공원", "박물관", "관광지", "펜션", "리조트", "놀이공원", "아울렛", "한식당", "치킨집", "베이커리")),
    
    JEJU("제주특별자치도", 3, listOf(
        RegionCoordinate(BigDecimal("33.4996"), BigDecimal("126.5312"), 15000, "제주시"),
        RegionCoordinate(BigDecimal("33.2541"), BigDecimal("126.5601"), 15000, "서귀포시"),
        RegionCoordinate(BigDecimal("33.5066"), BigDecimal("126.4914"), 10000, "애월읍"),
        RegionCoordinate(BigDecimal("33.4734"), BigDecimal("126.3402"), 10000, "한림읍"),
        RegionCoordinate(BigDecimal("33.3890"), BigDecimal("126.9423"), 10000, "표선면"),
        RegionCoordinate(BigDecimal("33.2348"), BigDecimal("126.4065"), 10000, "안덕면"),
        RegionCoordinate(BigDecimal("33.1862"), BigDecimal("126.3051"), 10000, "대정읍")
    ), listOf("카페", "레스토랑", "음식점", "관광지", "해변", "박물관", "펜션", "리조트", "흑돼지구이", "해산물", "감귤체험", "승마장", "골프장", "올레길")),
    
    BUSAN("부산광역시", 4, listOf(
        RegionCoordinate(BigDecimal("35.1796"), BigDecimal("129.0756"), 8000, "부산진구"),
        RegionCoordinate(BigDecimal("35.1547"), BigDecimal("129.1186"), 8000, "해운대구"),
        RegionCoordinate(BigDecimal("35.0979"), BigDecimal("129.0359"), 8000, "중구"),
        RegionCoordinate(BigDecimal("35.2332"), BigDecimal("129.0820"), 8000, "동래구"),
        RegionCoordinate(BigDecimal("35.1280"), BigDecimal("128.9430"), 8000, "서구"),
        RegionCoordinate(BigDecimal("35.2051"), BigDecimal("129.2177"), 8000, "기장군")
    ), listOf("카페", "레스토랑", "음식점", "해변", "박물관", "시장", "자갈치시장", "해산물", "밀면", "돼지국밥", "씨앗호떡", "관광지", "온천", "사찰", "영화관")),
    
    OTHER_REGIONS("기타지역", 5, listOf(
        // 대구
        RegionCoordinate(BigDecimal("35.8714"), BigDecimal("128.6014"), 8000, "대구광역시"),
        // 인천
        RegionCoordinate(BigDecimal("37.4563"), BigDecimal("126.7052"), 8000, "인천광역시"),
        // 광주
        RegionCoordinate(BigDecimal("35.1595"), BigDecimal("126.8526"), 8000, "광주광역시"),
        // 대전
        RegionCoordinate(BigDecimal("36.3504"), BigDecimal("127.3845"), 8000, "대전광역시"),
        // 울산
        RegionCoordinate(BigDecimal("35.5384"), BigDecimal("129.3114"), 8000, "울산광역시"),
        // 강원도 (춘천)
        RegionCoordinate(BigDecimal("37.8813"), BigDecimal("127.7298"), 10000, "강원도 춘천시"),
        // 충청북도 (청주)
        RegionCoordinate(BigDecimal("36.6424"), BigDecimal("127.4890"), 10000, "충청북도 청주시"),
        // 충청남도 (천안)
        RegionCoordinate(BigDecimal("36.8151"), BigDecimal("127.1139"), 10000, "충청남도 천안시"),
        // 전라북도 (전주)
        RegionCoordinate(BigDecimal("35.8242"), BigDecimal("127.1480"), 10000, "전라북도 전주시"),
        // 전라남도 (순천)
        RegionCoordinate(BigDecimal("34.9506"), BigDecimal("127.4876"), 10000, "전라남도 순천시"),
        // 경상북도 (포항)
        RegionCoordinate(BigDecimal("36.0190"), BigDecimal("129.3435"), 10000, "경상북도 포항시"),
        // 경상남도 (창원)
        RegionCoordinate(BigDecimal("35.2281"), BigDecimal("128.6811"), 10000, "경상남도 창원시")
    ), listOf("카페", "레스토랑", "음식점", "관광지", "박물관", "공원", "시장", "한식당", "전통음식", "지역특산품", "펜션", "온천"))
}

data class RegionCoordinate(
    val lat: BigDecimal,
    val lng: BigDecimal, 
    val radius: Int,
    val description: String
)

data class RegionalSearchContext(
    val region: KoreanRegion,
    val coordinate: RegionCoordinate,
    val query: String,
    val page: Int,
    val totalPages: Int = 0
)

data class RegionalProcessingState(
    val currentRegion: KoreanRegion,
    val completedRegions: Set<KoreanRegion> = emptySet(),
    val currentCoordinateIndex: Int = 0,
    val currentQueryIndex: Int = 0,
    val currentPage: Int = 1,
    val totalProcessedPlaces: Long = 0,
    val errors: MutableList<String> = mutableListOf()
)
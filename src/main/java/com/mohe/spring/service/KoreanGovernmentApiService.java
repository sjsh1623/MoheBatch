package com.mohe.spring.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohe.spring.dto.KoreanRegionDto;
import com.mohe.spring.dto.KoreanRegionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service for fetching Korean administrative regions from the Korean Government API
 * Data is fetched temporarily and not stored in the database
 */
@Service
public class KoreanGovernmentApiService {
    
    private static final Logger logger = LoggerFactory.getLogger(KoreanGovernmentApiService.class);
    
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    
    // Korean Government API service key (public data)
    private static final String SERVICE_KEY = "f5a3bcde8e1b032f6f0d36b525353d3e6b3843e9d4a478728219054bde74f20f";
    private static final String API_BASE_URL = "http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList";
    
    // Cache for regions (optional - can be disabled for always fresh data)
    private List<KoreanRegionDto> cachedRegions = null;
    private long lastFetchTime = 0;
    private static final long CACHE_DURATION_MS = 24 * 60 * 60 * 1000; // 24 hours
    
    public KoreanGovernmentApiService(@Value("${app.korean-api.cache-enabled:true}") boolean cacheEnabled) {
        this.webClient = WebClient.builder()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(1024 * 1024)) // 1MB buffer
            .build();
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * Fetch all Korean administrative regions from the government API
     * @return List of all administrative regions (temporary, not stored)
     */
    public List<KoreanRegionDto> fetchAllKoreanRegions() {
        // Return cached data if available and not expired
        if (cachedRegions != null && (System.currentTimeMillis() - lastFetchTime) < CACHE_DURATION_MS) {
            logger.info("🏛️ Returning cached Korean regions: {} regions", cachedRegions.size());
            return new ArrayList<>(cachedRegions);
        }
        
        logger.info("🚀 Fetching Korean administrative regions from government API...");
        
        List<KoreanRegionDto> allRegions = new ArrayList<>();
        int page = 1;
        Integer totalCount = null;
        
        try {
            while (true) {
                final int currentPage = page; // Create final variable for lambda
                logger.info("📖 Fetching page {} from Korean Government API...", currentPage);
                
                // Make API call
                String jsonResponse = webClient.get()
                    .uri(uriBuilder -> uriBuilder
                        .scheme("http")
                        .host("apis.data.go.kr")
                        .path("/1741000/StanReginCd/getStanReginCdList")
                        .queryParam("serviceKey", SERVICE_KEY)
                        .queryParam("pageNo", currentPage)
                        .queryParam("numOfRows", 1000)
                        .queryParam("type", "json")
                        .build())
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(30))
                    .block();
                
                if (jsonResponse == null || jsonResponse.trim().isEmpty()) {
                    logger.warn("⚠️ Empty response from Korean Government API at page {}", currentPage);
                    break;
                }
                
                // Parse response manually (API returns text/html content-type but JSON body)
                KoreanRegionResponse response = objectMapper.readValue(jsonResponse, KoreanRegionResponse.class);
                
                if (response.getStanReginCd() == null || response.getStanReginCd().size() < 2) {
                    logger.warn("⚠️ Invalid response structure at page {}", currentPage);
                    break;
                }
                
                // Extract data from response
                KoreanRegionResponse.StanReginCdItem dataItem = response.getStanReginCd().get(1);
                List<KoreanRegionDto> pageRegions = dataItem.getRow();
                
                if (pageRegions == null || pageRegions.isEmpty()) {
                    logger.info("📄 No more data at page {} - pagination complete", currentPage);
                    break;
                }
                
                // Get total count from first page
                if (totalCount == null && !response.getStanReginCd().isEmpty()) {
                    totalCount = response.getStanReginCd().get(0).getTotalCount();
                    if (totalCount != null) {
                        logger.info("📊 Korean Government API reports {} total administrative regions", totalCount);
                    }
                }
                
                allRegions.addAll(pageRegions);
                logger.info("✅ Page {} processed: {} regions (Total so far: {})", 
                          currentPage, pageRegions.size(), allRegions.size());
                
                // Check if we've reached the end (less than 1000 records)
                if (pageRegions.size() < 1000) {
                    logger.info("🏁 Reached end of data at page {} with {} records", currentPage, pageRegions.size());
                    break;
                }
                
                page++;
                
                // Safety limit to prevent infinite loops
                if (page > 50) {
                    logger.warn("⚠️ Reached safety limit of 50 pages, stopping");
                    break;
                }
                
                // Small delay to be respectful to the API
                Thread.sleep(100);
            }
            
            // Cache the results
            cachedRegions = new ArrayList<>(allRegions);
            lastFetchTime = System.currentTimeMillis();
            
            logger.info("🎉 Korean Government API fetch complete: {} total regions loaded", allRegions.size());
            
            return allRegions;
            
        } catch (WebClientException e) {
            logger.error("❌ Network error fetching Korean regions: {}", e.getMessage());
            return getHardcodedRegions();
        } catch (Exception e) {
            logger.error("❌ Error fetching Korean administrative regions", e);
            return getHardcodedRegions();
        }
    }

    /**
     * 정부 API 실패 시 하드코딩된 동 데이터 반환 (화재로 인한 서버 다운 대응)
     * 기존 API와 동일한 구조로 KoreanRegionDto 생성하여 호환성 보장
     */
    private List<KoreanRegionDto> getHardcodedRegions() {
        logger.warn("🔥 정부 API 서버 장애 - 하드코딩된 지역 데이터 사용");

        List<KoreanRegionDto> hardcodedRegions = new ArrayList<>();

        // 서울특별시 주요 동들
        String[] seoulDongs = {
            "강남구 신사동", "강남구 논현동", "강남구 압구정동", "강남구 청담동", "강남구 삼성동", "강남구 대치동",
            "서초구 서초동", "서초구 반포동", "서초구 방배동", "서초구 잠원동",
            "송파구 잠실동", "송파구 문정동", "송파구 가락동", "송파구 석촌동",
            "강동구 천호동", "강동구 성내동", "강동구 강일동", "강동구 암사동",
            "마포구 홍대동", "마포구 합정동", "마포구 상암동", "마포구 연남동",
            "용산구 이태원동", "용산구 한남동", "용산구 청파동", "용산구 효창동",
            "중구 명동", "중구 충무로", "중구 을지로", "중구 신당동",
            "종로구 종로동", "종로구 인사동", "종로구 삼청동", "종로구 혜화동",
            "성동구 성수동", "성동구 왕십리동", "성동구 금호동", "성동구 행당동"
        };

        // 경기도 주요 동들
        String[] gyeonggiDongs = {
            "수원시 영통구 영통동", "수원시 영통구 매탄동", "수원시 팔달구 행궁동", "수원시 장안구 정자동",
            "성남시 분당구 정자동", "성남시 분당구 서현동", "성남시 분당구 이매동", "성남시 수정구 신흥동",
            "안양시 동안구 평촌동", "안양시 만안구 안양동", "안양시 동안구 비산동",
            "부천시 원미구 중동", "부천시 오정구 원종동", "부천시 소사구 소사동",
            "고양시 일산동구 백석동", "고양시 일산서구 주엽동", "고양시 덕양구 화정동",
            "용인시 기흥구 기흥동", "용인시 수지구 수지동", "용인시 처인구 김량장동",
            "화성시 동탄면 동탄동", "화성시 봉담읍 봉담리", "화성시 태안읍 태안리"
        };

        // 제주도 주요 동들
        String[] jejuDongs = {
            "제주시 일도1동", "제주시 일도2동", "제주시 이도1동", "제주시 이도2동",
            "제주시 삼도1동", "제주시 삼도2동", "제주시 용담1동", "제주시 용담2동",
            "제주시 건입동", "제주시 화북동", "제주시 삼양동", "제주시 봉개동",
            "서귀포시 서귀동", "서귀포시 정방동", "서귀포시 중앙동", "서귀포시 천지동",
            "서귀포시 효돈동", "서귀포시 영천동", "서귀포시 동홍동", "서귀포시 서홍동"
        };

        int regionIndex = 1;

        // 서울 데이터 생성
        for (String dong : seoulDongs) {
            KoreanRegionDto region = new KoreanRegionDto();
            region.setRegionCode(String.format("11%05d", regionIndex++));
            region.setLocationName("서울특별시 " + dong);
            region.setSidoCode("11"); // 서울특별시
            region.setSigunguCode("680"); // 강남구 예시
            region.setUmdCode("001"); // 동 레벨 (isDongLevel() true가 되도록)
            region.setRiCode("00"); // 동 레벨
            hardcodedRegions.add(region);
        }

        // 경기도 데이터 생성
        for (String dong : gyeonggiDongs) {
            KoreanRegionDto region = new KoreanRegionDto();
            region.setRegionCode(String.format("41%05d", regionIndex++));
            region.setLocationName("경기도 " + dong);
            region.setSidoCode("41"); // 경기도
            region.setSigunguCode("460"); // 수원시 예시
            region.setUmdCode("001"); // 동 레벨
            region.setRiCode("00"); // 동 레벨
            hardcodedRegions.add(region);
        }

        // 제주도 데이터 생성
        for (String dong : jejuDongs) {
            KoreanRegionDto region = new KoreanRegionDto();
            region.setRegionCode(String.format("50%05d", regionIndex++));
            region.setLocationName("제주특별자치도 " + dong);
            region.setSidoCode("50"); // 제주특별자치도
            region.setSigunguCode("110"); // 제주시 예시
            region.setUmdCode("001"); // 동 레벨
            region.setRiCode("00"); // 동 레벨
            hardcodedRegions.add(region);
        }

        logger.info("🏛️ 하드코딩된 지역 데이터 로드 완료: {} 개 지역", hardcodedRegions.size());
        return hardcodedRegions;
    }

    /**
     * Get only dong-level administrative regions (filtered)
     * @return List of dong-level regions only
     */
    public List<KoreanRegionDto> fetchDongLevelRegions() {
        List<KoreanRegionDto> allRegions = fetchAllKoreanRegions();
        
        List<KoreanRegionDto> dongRegions = allRegions.stream()
            .filter(KoreanRegionDto::isDongLevel)
            .collect(Collectors.toList());
            
        logger.info("🎯 Filtered {} dong-level regions from {} total regions", 
                   dongRegions.size(), allRegions.size());
        
        return dongRegions;
    }
    
    /**
     * Get simple location names for search queries
     * @return List of location names (e.g., "강남구", "신사동")
     */
    public List<String> fetchLocationNamesForSearch() {
        List<KoreanRegionDto> dongRegions = fetchDongLevelRegions();
        
        List<String> locationNames = dongRegions.stream()
            .map(KoreanRegionDto::getSimpleLocationName)
            .filter(name -> name != null && !name.trim().isEmpty())
            .distinct()
            .collect(Collectors.toList());
            
        logger.info("📍 Extracted {} unique location names for search queries", locationNames.size());
        logger.info("🎯 Sample locations: {}", 
                   locationNames.stream().limit(10).collect(Collectors.joining(", ")));
        
        return locationNames;
    }
    
    /**
     * Clear the cache to force fresh data fetch
     */
    public void clearCache() {
        cachedRegions = null;
        lastFetchTime = 0;
        logger.info("🗑️ Korean regions cache cleared");
    }
    
    /**
     * Get cache statistics
     */
    public String getCacheStatus() {
        if (cachedRegions == null) {
            return "No cache";
        }
        long ageMs = System.currentTimeMillis() - lastFetchTime;
        long ageMinutes = ageMs / (60 * 1000);
        return String.format("Cached: %d regions, age: %d minutes", cachedRegions.size(), ageMinutes);
    }
}
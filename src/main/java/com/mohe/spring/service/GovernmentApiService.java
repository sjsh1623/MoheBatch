package com.mohe.spring.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 정부 공공데이터 API 연동 서비스
 * - 행정안전부 행정구역 API
 * - VWORLD 지도 API
 * - 통계청 SGIS API
 */
@Service
public class GovernmentApiService {

    private static final Logger logger = LoggerFactory.getLogger(GovernmentApiService.class);

    @Value("${GOVT_API_KEY:}")
    private String govtApiKey;

    @Value("${VWORLD_API_KEY:}")
    private String vworldApiKey;

    @Value("${SGIS_API_KEY:}")
    private String sgisApiKey;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    /**
     * 행정안전부 행정구역 API를 통해 실제 지역 정보 조회
     */
    public List<RegionInfo> getAdministrativeRegions() {
        List<RegionInfo> regions = new ArrayList<>();

        try {
            // 행정안전부 행정구역 API 호출
            String url = String.format(
                "http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList" +
                "?serviceKey=%s&pageNo=1&numOfRows=100&type=json&locatadd_nm=시도",
                govtApiKey
            );

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(30))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                JsonNode root = objectMapper.readTree(response.body());
                JsonNode items = root.path("StanReginCd").path(1).path("row");

                if (items.isArray()) {
                    for (JsonNode item : items) {
                        String regionName = item.path("locatadd_nm").asText();
                        String regionCode = item.path("region_cd").asText();

                        if (!regionName.isEmpty()) {
                            // 좌표는 별도 API나 기본값 사용
                            RegionInfo region = getRegionCoordinates(regionName, regionCode);
                            if (region != null) {
                                regions.add(region);
                            }
                        }
                    }
                }
            } else {
                logger.warn("정부 API 호출 실패: HTTP {}", response.statusCode());
                // Fallback: 기본 지역 목록 사용
                regions = getFallbackRegions();
            }

        } catch (Exception e) {
            logger.error("정부 API 호출 중 오류 발생", e);
            // Fallback: 기본 지역 목록 사용
            regions = getFallbackRegions();
        }

        logger.info("🏛️ 정부 API로부터 {}개 지역 정보 수집", regions.size());
        return regions;
    }

    /**
     * VWORLD API를 통해 지역의 좌표 정보 조회
     */
    private RegionInfo getRegionCoordinates(String regionName, String regionCode) {
        try {
            if (vworldApiKey == null || vworldApiKey.isEmpty()) {
                return createDefaultRegion(regionName, regionCode);
            }

            String url = String.format(
                "http://api.vworld.kr/req/address" +
                "?service=address&request=getcoord&version=2.0&crs=epsg:4326" +
                "&address=%s&format=json&type=road&key=%s",
                java.net.URLEncoder.encode(regionName, "UTF-8"),
                vworldApiKey
            );

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                JsonNode root = objectMapper.readTree(response.body());
                JsonNode point = root.path("response").path("result").path("point");

                if (!point.isMissingNode()) {
                    double longitude = point.path("x").asDouble();
                    double latitude = point.path("y").asDouble();

                    return new RegionInfo(regionName, regionCode, latitude, longitude);
                }
            }

        } catch (Exception e) {
            logger.debug("좌표 조회 실패 for {}: {}", regionName, e.getMessage());
        }

        // Fallback: 기본 좌표 사용
        return createDefaultRegion(regionName, regionCode);
    }

    /**
     * 통계청 SGIS API를 통해 지역별 통계 정보 조회 (선택적)
     */
    public List<String> getPopularCategoriesByRegion(String regionCode) {
        List<String> categories = new ArrayList<>();

        try {
            // SGIS API 호출 로직 (실제 구현 시 필요)
            // 현재는 기본 카테고리 반환
            categories.add("카페");
            categories.add("맛집");
            categories.add("쇼핑");

        } catch (Exception e) {
            logger.debug("지역별 카테고리 조회 실패 for {}: {}", regionCode, e.getMessage());
        }

        return categories;
    }

    /**
     * API 호출 실패 시 사용할 기본 지역 목록
     */
    private List<RegionInfo> getFallbackRegions() {
        List<RegionInfo> regions = new ArrayList<>();

        // 주요 도시의 기본 좌표
        regions.add(new RegionInfo("서울특별시", "11", 37.5665, 126.9780));
        regions.add(new RegionInfo("부산광역시", "26", 35.1796, 129.0756));
        regions.add(new RegionInfo("대구광역시", "27", 35.8714, 128.6014));
        regions.add(new RegionInfo("인천광역시", "28", 37.4563, 126.7052));
        regions.add(new RegionInfo("광주광역시", "29", 35.1595, 126.8526));
        regions.add(new RegionInfo("대전광역시", "30", 36.3504, 127.3845));
        regions.add(new RegionInfo("울산광역시", "31", 35.5384, 129.3114));
        regions.add(new RegionInfo("세종특별자치시", "36", 36.4800, 127.2890));
        regions.add(new RegionInfo("경기도", "41", 37.4138, 127.5183));
        regions.add(new RegionInfo("강원도", "42", 37.8228, 128.1555));
        regions.add(new RegionInfo("충청북도", "43", 36.6357, 127.4914));
        regions.add(new RegionInfo("충청남도", "44", 36.5184, 126.8000));
        regions.add(new RegionInfo("전라북도", "45", 35.7175, 127.1530));
        regions.add(new RegionInfo("전라남도", "46", 34.8679, 126.9910));
        regions.add(new RegionInfo("경상북도", "47", 36.4919, 128.8889));
        regions.add(new RegionInfo("경상남도", "48", 35.4606, 128.2132));
        regions.add(new RegionInfo("제주특별자치도", "50", 33.4996, 126.5312));

        logger.info("📍 Fallback 지역 목록 사용: {}개 지역", regions.size());
        return regions;
    }

    /**
     * 기본 좌표를 가진 지역 정보 생성
     */
    private RegionInfo createDefaultRegion(String regionName, String regionCode) {
        // 간단한 매핑 로직 (실제로는 더 정교한 매핑 필요)
        double latitude = 37.5665; // 서울 기본값
        double longitude = 126.9780;

        if (regionName.contains("부산")) {
            latitude = 35.1796;
            longitude = 129.0756;
        } else if (regionName.contains("대구")) {
            latitude = 35.8714;
            longitude = 128.6014;
        } else if (regionName.contains("인천")) {
            latitude = 37.4563;
            longitude = 126.7052;
        }
        // ... 추가 매핑 로직

        return new RegionInfo(regionName, regionCode, latitude, longitude);
    }

    /**
     * 지역 정보를 담는 클래스
     */
    public static class RegionInfo {
        private final String name;
        private final String code;
        private final double latitude;
        private final double longitude;

        public RegionInfo(String name, String code, double latitude, double longitude) {
            this.name = name;
            this.code = code;
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public String getName() { return name; }
        public String getCode() { return code; }
        public double getLatitude() { return latitude; }
        public double getLongitude() { return longitude; }

        @Override
        public String toString() {
            return String.format("%s[%s] (%.4f, %.4f)", name, code, latitude, longitude);
        }
    }
}
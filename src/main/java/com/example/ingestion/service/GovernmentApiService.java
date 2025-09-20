package com.example.ingestion.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 정부 행정구역 API 서비스 (배치 전용)
 * 대한민국 전국 시/군/구/동 단위의 행정구역 정보를 수집합니다
 */
@Service
public class GovernmentApiService {

    private static final Logger logger = LoggerFactory.getLogger(GovernmentApiService.class);

    private final WebClient webClient;
    private final String serviceKey;

    public GovernmentApiService(
            WebClient webClient,
            @Value("${GOVT_API_KEY:TEMP_KEY}") String serviceKey
    ) {
        this.webClient = webClient;
        this.serviceKey = serviceKey;
    }

    /**
     * 전국 시/도 목록을 가져옵니다
     */
    public Flux<AdministrativeRegion> getAllSidos() {
        logger.info("🏛️ Fetching all sido (시/도) data from government API");

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .scheme("https")
                        .host("apis.data.go.kr")
                        .path("/1741000/StanReginCd/getStanReginCdList")
                        .queryParam("serviceKey", serviceKey)
                        .queryParam("pageNo", "1")
                        .queryParam("numOfRows", "100")
                        .queryParam("type", "json")
                        .queryParam("locatadd_nm", "")
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofSeconds(10)))
                .flux()
                .flatMapIterable(this::parseSidoData)
                .doOnNext(sido -> logger.debug("Found sido: {}", sido.getSido()))
                .doOnComplete(() -> logger.info("✅ Completed fetching sido data"));
    }

    /**
     * 특정 시/도의 시/군/구 목록을 가져옵니다
     */
    public Flux<AdministrativeRegion> getSigunguBySido(String sidoCode) {
        logger.debug("🏘️ Fetching sigungu data for sido code: {}", sidoCode);

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .scheme("https")
                        .host("apis.data.go.kr")
                        .path("/1741000/StanReginCd/getStanReginCdList")
                        .queryParam("serviceKey", serviceKey)
                        .queryParam("pageNo", "1")
                        .queryParam("numOfRows", "500")
                        .queryParam("type", "json")
                        .queryParam("umd_cd", sidoCode.substring(0, 2)) // 시/도 코드 2자리
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofSeconds(10)))
                .flux()
                .flatMapIterable(this::parseSigunguData)
                .doOnNext(sigungu -> logger.debug("Found sigungu: {} in {}",
                        sigungu.getSigungu(), sigungu.getSido()));
    }

    /**
     * 특정 시/군/구의 동 목록을 가져옵니다
     */
    public Flux<AdministrativeRegion> getDongBySigungu(String sigunguCode) {
        logger.debug("🏠 Fetching dong data for sigungu code: {}", sigunguCode);

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .scheme("https")
                        .host("apis.data.go.kr")
                        .path("/1741000/StanReginCd/getStanReginCdList")
                        .queryParam("serviceKey", serviceKey)
                        .queryParam("pageNo", "1")
                        .queryParam("numOfRows", "1000")
                        .queryParam("type", "json")
                        .queryParam("umd_cd", sigunguCode.substring(0, 5)) // 시/군/구 코드 5자리
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofSeconds(10)))
                .flux()
                .flatMapIterable(this::parseDongData)
                .doOnNext(dong -> logger.debug("Found dong: {} in {}/{}",
                        dong.getDong(), dong.getSido(), dong.getSigungu()));
    }

    /**
     * 전국 모든 행정구역 (시/도/시군구/동)을 계층적으로 수집합니다
     */
    public Flux<AdministrativeRegion> getAllAdministrativeRegions() {
        logger.info("🌍 Starting comprehensive administrative region collection");

        return getAllSidos()
                .delayElements(Duration.ofMillis(500)) // Rate limiting
                .flatMap(sido -> {
                    return getSigunguBySido(sido.getRegionCode())
                            .delayElements(Duration.ofMillis(300))
                            .flatMap(sigungu -> {
                                return getDongBySigungu(sigungu.getRegionCode())
                                        .delayElements(Duration.ofMillis(100))
                                        .map(dong -> AdministrativeRegion.builder()
                                                .sido(sido.getSido())
                                                .sigungu(sigungu.getSigungu())
                                                .dong(dong.getDong())
                                                .regionCode(dong.getRegionCode())
                                                .build());
                            });
                })
                .doFinally(signalType -> logger.info("🏁 Administrative region collection completed with signal: {}", signalType));
    }

    /**
     * 시/도 데이터 파싱
     */
    private List<AdministrativeRegion> parseSidoData(JsonNode response) {
        List<AdministrativeRegion> sidos = new ArrayList<>();

        if (!response.has("StanReginCd") || !response.get("StanReginCd").has("row")) {
            logger.warn("Invalid sido response structure");
            return sidos;
        }

        JsonNode rows = response.get("StanReginCd").get("row");
        if (!rows.isArray()) {
            logger.warn("Sido rows is not an array");
            return sidos;
        }

        for (JsonNode row : rows) {
            try {
                String regionCode = row.path("region_cd").asText();
                String sidoName = row.path("sido_nm").asText();

                // 시/도 레벨만 필터링 (코드 길이가 2자리)
                if (regionCode.length() == 2 && !sidoName.isEmpty()) {
                    sidos.add(AdministrativeRegion.builder()
                            .sido(sidoName)
                            .regionCode(regionCode)
                            .build());
                }
            } catch (Exception e) {
                logger.warn("Error parsing sido data: {}", e.getMessage());
            }
        }

        logger.debug("Parsed {} sido regions", sidos.size());
        return sidos;
    }

    /**
     * 시/군/구 데이터 파싱
     */
    private List<AdministrativeRegion> parseSigunguData(JsonNode response) {
        List<AdministrativeRegion> sigungus = new ArrayList<>();

        if (!response.has("StanReginCd") || !response.get("StanReginCd").has("row")) {
            logger.warn("Invalid sigungu response structure");
            return sigungus;
        }

        JsonNode rows = response.get("StanReginCd").get("row");
        if (!rows.isArray()) {
            logger.warn("Sigungu rows is not an array");
            return sigungus;
        }

        for (JsonNode row : rows) {
            try {
                String regionCode = row.path("region_cd").asText();
                String sidoName = row.path("sido_nm").asText();
                String sigunguName = row.path("sigungu_nm").asText();

                // 시/군/구 레벨만 필터링 (코드 길이가 5자리)
                if (regionCode.length() == 5 && !sigunguName.isEmpty()) {
                    sigungus.add(AdministrativeRegion.builder()
                            .sido(sidoName)
                            .sigungu(sigunguName)
                            .regionCode(regionCode)
                            .build());
                }
            } catch (Exception e) {
                logger.warn("Error parsing sigungu data: {}", e.getMessage());
            }
        }

        logger.debug("Parsed {} sigungu regions", sigungus.size());
        return sigungus;
    }

    /**
     * 동 데이터 파싱
     */
    private List<AdministrativeRegion> parseDongData(JsonNode response) {
        List<AdministrativeRegion> dongs = new ArrayList<>();

        if (!response.has("StanReginCd") || !response.get("StanReginCd").has("row")) {
            logger.warn("Invalid dong response structure");
            return dongs;
        }

        JsonNode rows = response.get("StanReginCd").get("row");
        if (!rows.isArray()) {
            logger.warn("Dong rows is not an array");
            return dongs;
        }

        for (JsonNode row : rows) {
            try {
                String regionCode = row.path("region_cd").asText();
                String sidoName = row.path("sido_nm").asText();
                String sigunguName = row.path("sigungu_nm").asText();
                String dongName = row.path("eupmyeondong_nm").asText();

                // 동 레벨만 필터링 (코드 길이가 8자리 이상)
                if (regionCode.length() >= 8 && !dongName.isEmpty()) {
                    dongs.add(AdministrativeRegion.builder()
                            .sido(sidoName)
                            .sigungu(sigunguName)
                            .dong(dongName)
                            .regionCode(regionCode)
                            .build());
                }
            } catch (Exception e) {
                logger.warn("Error parsing dong data: {}", e.getMessage());
            }
        }

        logger.debug("Parsed {} dong regions", dongs.size());
        return dongs;
    }

    /**
     * 행정구역 정보를 담는 DTO 클래스
     */
    public static class AdministrativeRegion {
        private String sido;
        private String sigungu;
        private String dong;
        private String regionCode;

        public AdministrativeRegion() {}

        private AdministrativeRegion(Builder builder) {
            this.sido = builder.sido;
            this.sigungu = builder.sigungu;
            this.dong = builder.dong;
            this.regionCode = builder.regionCode;
        }

        public static Builder builder() {
            return new Builder();
        }

        // Getters
        public String getSido() { return sido; }
        public String getSigungu() { return sigungu; }
        public String getDong() { return dong; }
        public String getRegionCode() { return regionCode; }

        // Setters
        public void setSido(String sido) { this.sido = sido; }
        public void setSigungu(String sigungu) { this.sigungu = sigungu; }
        public void setDong(String dong) { this.dong = dong; }
        public void setRegionCode(String regionCode) { this.regionCode = regionCode; }

        public String getFullAddress() {
            StringBuilder address = new StringBuilder();
            if (sido != null) address.append(sido);
            if (sigungu != null) address.append(" ").append(sigungu);
            if (dong != null) address.append(" ").append(dong);
            return address.toString().trim();
        }

        @Override
        public String toString() {
            return String.format("AdministrativeRegion{sido='%s', sigungu='%s', dong='%s', code='%s'}",
                    sido, sigungu, dong, regionCode);
        }

        public static class Builder {
            private String sido;
            private String sigungu;
            private String dong;
            private String regionCode;

            public Builder sido(String sido) {
                this.sido = sido;
                return this;
            }

            public Builder sigungu(String sigungu) {
                this.sigungu = sigungu;
                return this;
            }

            public Builder dong(String dong) {
                this.dong = dong;
                return this;
            }

            public Builder regionCode(String regionCode) {
                this.regionCode = regionCode;
                return this;
            }

            public AdministrativeRegion build() {
                return new AdministrativeRegion(this);
            }
        }
    }
}
package com.mohe.batch.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohe.batch.dto.crawling.CrawledDataDto;
import com.mohe.batch.dto.crawling.CrawlingResponse;
import com.mohe.batch.dto.crawling.MenuDataDto;
import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class CrawlingService {

    private static final Logger log = LoggerFactory.getLogger(CrawlingService.class);

    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    public CrawlingService(
            WebClient.Builder webClientBuilder,
            ObjectMapper objectMapper,
            @Value("${crawler.base-url:http://localhost:4000}") String baseUrl,
            @Value("${crawler.timeout-minutes:2}") int timeoutMinutes) {

        log.info("CrawlingService initialized with baseUrl: {}, timeout: {} minutes", baseUrl, timeoutMinutes);

        HttpClient httpClient = HttpClient.create()
                .responseTimeout(Duration.ofMinutes(timeoutMinutes))
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000)  // 연결 타임아웃 30초
                .option(ChannelOption.SO_KEEPALIVE, true)
                .doOnConnected(conn -> conn
                        .addHandlerLast(new ReadTimeoutHandler(timeoutMinutes * 60, TimeUnit.SECONDS))
                        .addHandlerLast(new WriteTimeoutHandler(120, TimeUnit.SECONDS)));  // 쓰기 타임아웃 2분

        this.webClient = webClientBuilder
                .baseUrl(baseUrl)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
        this.objectMapper = objectMapper;
    }

    public Mono<CrawlingResponse<CrawledDataDto>> crawlPlaceData(String searchQuery, String placeName) {
        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("searchQuery", searchQuery + " " + placeName);
        requestBody.put("placeName", placeName);

        return webClient.post()
                .uri("/api/v1/place")
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(Map.class)
                .map(responseMap -> {
                    CrawlingResponse<CrawledDataDto> response = new CrawlingResponse<>();
                    response.setSuccess((Boolean) responseMap.get("success"));
                    response.setMessage((String) responseMap.get("message"));
                    CrawledDataDto data = objectMapper.convertValue(
                            responseMap.get("data"),
                            new TypeReference<CrawledDataDto>() {}
                    );
                    response.setData(data);
                    return response;
                })
                .onErrorResume(error -> {
                    log.error("Crawling error for '{}': {}", placeName, error.getMessage());
                    CrawlingResponse<CrawledDataDto> errorResponse = new CrawlingResponse<>();
                    errorResponse.setSuccess(false);
                    errorResponse.setMessage(error.getMessage());
                    errorResponse.setData(null);
                    return Mono.just(errorResponse);
                });
    }

    public Mono<CrawlingResponse<MenuDataDto>> crawlMenuData(String searchQuery, String placeName) {
        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("searchQuery", searchQuery + " " + placeName);
        requestBody.put("placeName", placeName);
        // 메뉴 제한 없음 - 모든 메뉴 크롤링

        return webClient.post()
                .uri("/api/v1/place/menus")
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(Map.class)
                .map(responseMap -> {
                    CrawlingResponse<MenuDataDto> response = new CrawlingResponse<>();
                    response.setSuccess((Boolean) responseMap.get("success"));
                    response.setMessage((String) responseMap.get("message"));
                    MenuDataDto data = objectMapper.convertValue(
                            responseMap.get("data"),
                            new TypeReference<MenuDataDto>() {}
                    );
                    response.setData(data);
                    return response;
                })
                .onErrorResume(error -> {
                    log.error("Menu crawling error for '{}': {}", placeName, error.getMessage());
                    CrawlingResponse<MenuDataDto> errorResponse = new CrawlingResponse<>();
                    errorResponse.setSuccess(false);
                    errorResponse.setMessage(error.getMessage());
                    errorResponse.setData(null);
                    return Mono.just(errorResponse);
                });
    }
}

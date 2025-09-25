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
            return Collections.emptyList();
        } catch (Exception e) {
            logger.error("❌ Error fetching Korean administrative regions", e);
            return Collections.emptyList();
        }
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
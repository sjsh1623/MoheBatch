package com.example.ingestion.batch.reader;

import com.example.ingestion.dto.GooglePlaceDetail;
import com.example.ingestion.dto.NaverPlaceItem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Real API place reader that fetches data from Naver Local Search API
 * Replaces MockPlaceReader for production use
 */
@Component
public class RealApiPlaceReader implements ItemReader<EnrichedPlace> {

    private static final Logger logger = LoggerFactory.getLogger(RealApiPlaceReader.class);

    @Value("${NAVER_CLIENT_ID}")
    private String naverClientId;

    @Value("${NAVER_CLIENT_SECRET}")
    private String naverClientSecret;

    @Value("${GOOGLE_PLACES_API_KEY}")
    private String googleApiKey;

    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    private List<EnrichedPlace> fetchedPlaces;
    private final AtomicInteger currentIndex = new AtomicInteger(0);
    private boolean initialized = false;

    // Seoul coordinates for place fetching
    private final String[] seoulLocations = {
        "강남역", "홍대입구", "명동", "이태원", "종로", "잠실", "신촌", "건대입구"
    };

    private final String[] searchCategories = {
        "카페", "레스토랑", "관광명소", "문화시설", "공원"
    };

    public RealApiPlaceReader() {
        this.webClient = WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024))
                .build();
        this.objectMapper = new ObjectMapper();
        this.fetchedPlaces = new ArrayList<>();
    }

    @Override
    public EnrichedPlace read() {
        if (!initialized) {
            initializeData();
            initialized = true;
        }

        int index = currentIndex.getAndIncrement();

        if (index >= fetchedPlaces.size()) {
            logger.info("✅ RealApiPlaceReader finished - read {} places", index);
            return null; // End of data
        }

        EnrichedPlace place = fetchedPlaces.get(index);
        logger.debug("📖 Reading real place #{}: {}", index + 1, place.getNaverPlace().getCleanTitle());

        return place;
    }

    private void initializeData() {
        logger.info("🌐 Initializing RealApiPlaceReader - fetching data from Naver API...");

        try {
            // Fetch places from different locations and categories
            for (String location : seoulLocations) {
                for (String category : searchCategories) {
                    fetchPlacesFromNaver(location + " " + category);

                    // Add delay to respect API rate limits
                    Thread.sleep(1000);

                    // Limit total places to prevent overwhelming
                    if (fetchedPlaces.size() >= 50) {
                        break;
                    }
                }
                if (fetchedPlaces.size() >= 50) {
                    break;
                }
            }

            logger.info("🏗️ Fetched {} real places from Naver API", fetchedPlaces.size());

        } catch (Exception e) {
            logger.error("❌ Failed to fetch places from Naver API", e);
            // Fallback to a small set of real places if API fails
            createFallbackPlaces();
        }
    }

    private void fetchPlacesFromNaver(String query) {
        try {
            String response = webClient.get()
                    .uri("https://openapi.naver.com/v1/search/local.json?query={query}&display=10&start=1&sort=random", query)
                    .header("X-Naver-Client-Id", naverClientId)
                    .header("X-Naver-Client-Secret", naverClientSecret)
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(10))
                    .block();

            if (response != null) {
                parseNaverResponse(response);
            }

        } catch (Exception e) {
            logger.warn("⚠️ Failed to fetch places for query: {} - {}", query, e.getMessage());
        }
    }

    private void parseNaverResponse(String response) {
        try {
            JsonNode root = objectMapper.readTree(response);
            JsonNode items = root.path("items");

            for (JsonNode item : items) {
                NaverPlaceItem naverPlace = new NaverPlaceItem();
                naverPlace.setTitle(item.path("title").asText());
                naverPlace.setCategory(item.path("category").asText());
                naverPlace.setAddress(item.path("address").asText());
                naverPlace.setRoadAddress(item.path("roadAddress").asText());
                naverPlace.setDescription(item.path("description").asText());
                naverPlace.setTelephone(item.path("telephone").asText());
                naverPlace.setMapx(item.path("mapx").asText());
                naverPlace.setMapy(item.path("mapy").asText());

                // Convert to EnrichedPlace (without Google data for now)
                EnrichedPlace enrichedPlace = new EnrichedPlace(naverPlace, null);
                fetchedPlaces.add(enrichedPlace);
            }

        } catch (Exception e) {
            logger.error("❌ Failed to parse Naver API response", e);
        }
    }

    private void createFallbackPlaces() {
        logger.info("🔄 Creating fallback places as API fetch failed");

        // Create a few real places as fallback
        fetchedPlaces.add(createRealPlace("스타벅스 강남역점", "카페", "서울시 강남구 강남대로 390", 37.4979, 127.0276));
        fetchedPlaces.add(createRealPlace("경복궁", "문화시설", "서울시 종로구 사직로 161", 37.5796, 126.9770));
        fetchedPlaces.add(createRealPlace("한강공원", "공원", "서울시 용산구 이촌로 72", 37.5219, 126.9895));
        fetchedPlaces.add(createRealPlace("명동성당", "종교시설", "서울시 중구 명동길 74", 37.5636, 126.9748));
        fetchedPlaces.add(createRealPlace("동대문디자인플라자", "문화시설", "서울시 중구 을지로 281", 37.5662, 127.0090));

        logger.info("✅ Created {} fallback places", fetchedPlaces.size());
    }

    private EnrichedPlace createRealPlace(String name, String category, String address, double lat, double lng) {
        NaverPlaceItem naverPlace = new NaverPlaceItem();
        naverPlace.setTitle(name);
        naverPlace.setCategory(category);
        naverPlace.setAddress(address);
        naverPlace.setDescription(String.format("%s - %s에 위치한 %s", name, address, category));
        naverPlace.setMapx(String.valueOf(Math.round(lng * 10000000)));
        naverPlace.setMapy(String.valueOf(Math.round(lat * 10000000)));

        return new EnrichedPlace(naverPlace, null);
    }

    /**
     * Reset reader for multiple runs
     */
    public void reset() {
        currentIndex.set(0);
        initialized = false;
        fetchedPlaces.clear();
        logger.info("🔄 RealApiPlaceReader reset - ready to fetch new data");
    }

    public int getTotalPlaces() {
        return fetchedPlaces.size();
    }
}
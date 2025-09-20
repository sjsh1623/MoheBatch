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

    // Dynamic search parameters - no hard coding
    private final AtomicInteger searchOffset = new AtomicInteger(1);

    @Value("${APP_EXTERNAL_GOVERNMENT_SERVICE_KEY:}")
    private String govApiKey;

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
            // Reset index for each batch execution
            currentIndex.set(0);
            fetchedPlaces = new ArrayList<>();
            initializeData();
            initialized = true;
        }

        int index = currentIndex.getAndIncrement();

        if (index >= fetchedPlaces.size()) {
            logger.info("✅ RealApiPlaceReader finished - read {} places", index);
            // Reset for next execution
            initialized = false;
            return null; // End of data
        }

        EnrichedPlace place = fetchedPlaces.get(index);
        logger.debug("📖 Reading real place #{}: {}", index + 1, place.getNaverPlace().getCleanTitle());

        return place;
    }

    private void initializeData() {
        logger.info("🌐 Initializing RealApiPlaceReader - fetching data from Government API + Naver API...");

        try {
            // 1. Get regions from Government API
            List<String> regions = fetchRegionsFromGovAPI();
            logger.info("🏛️ Using {} regions for search", regions.size());

            // 2. For each region, search with different categories on Naver
            String[] categories = {"카페", "공원", "바", "레스토랑", "문화시설", "관광명소", "병원", "약국", "서점", "마트"};

            int queryCount = 0;
            for (String region : regions) {
                for (String category : categories) {
                    String searchQuery = region + " " + category;
                    logger.info("🔍 Searching Naver API with query: '{}'", searchQuery);
                    fetchPlacesFromNaver(searchQuery);
                    queryCount++;

                    // Add delay to respect API rate limits
                    Thread.sleep(200);

                    // Stop when we have enough places
                    if (fetchedPlaces.size() >= 50) {
                        logger.info("✅ Collected {} places with {} queries", fetchedPlaces.size(), queryCount);
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

    private List<String> fetchRegionsFromGovAPI() {
        List<String> regions = new ArrayList<>();

        logger.info("🏛️ Attempting to fetch regions from Government API...");
        logger.info("🔑 Government API key configured: {}", (govApiKey != null && !govApiKey.isEmpty()) ? "YES" : "NO");

        try {
            if (govApiKey == null || govApiKey.isEmpty()) {
                logger.warn("⚠️ Government API key not configured, using fallback regions");
                return getDefaultRegions();
            }

            // Use dynamic search offset to get different regions each time
            int currentOffset = searchOffset.getAndAdd(5);
            if (currentOffset > 100) {
                searchOffset.set(1); // Reset after reaching limit
                currentOffset = 1;
            }

            int pageNo = (currentOffset / 5) + 1;
            String apiUrl = String.format("http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList?serviceKey=%s&pageNo=%d&numOfRows=5&type=json",
                                        govApiKey, pageNo);
            logger.info("🌐 Calling Government API: page={}, offset={}", pageNo, currentOffset);

            String response = webClient.get()
                    .uri(apiUrl)
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(15))
                    .block();

            if (response != null) {
                logger.info("✅ Government API response received: {} characters", response.length());
                regions = parseGovResponse(response);
                logger.info("🏛️ Parsed {} regions from Government API", regions.size());
            } else {
                logger.warn("⚠️ Government API returned null response");
            }

        } catch (Exception e) {
            logger.error("❌ Failed to fetch regions from Government API: {}", e.getMessage(), e);
            regions = getDefaultRegions();
        }

        return regions.isEmpty() ? getDefaultRegions() : regions;
    }

    private List<String> parseGovResponse(String response) {
        List<String> regions = new ArrayList<>();
        try {
            JsonNode root = objectMapper.readTree(response);
            logger.info("🔍 Government API response structure: {}", root.toString().substring(0, Math.min(300, root.toString().length())));

            // Try standard Korean Government API structure first
            JsonNode items = null;

            // Standard public data portal structure: response > body > items > item
            if (root.has("response")) {
                JsonNode response_node = root.path("response");
                logger.debug("📋 Found response node");

                if (response_node.has("body")) {
                    JsonNode body = response_node.path("body");
                    logger.debug("📋 Found body node");

                    if (body.has("items")) {
                        JsonNode itemsContainer = body.path("items");
                        if (itemsContainer.has("item")) {
                            items = itemsContainer.path("item");
                            logger.info("🏛️ Found items in standard structure: {}", items.size());
                        }
                    }
                }
            }

            // Alternative structure: StanReginCd (specific to region code API)
            if (items == null && root.has("StanReginCd")) {
                JsonNode stanReginCd = root.path("StanReginCd");
                logger.debug("📋 Found StanReginCd node");

                if (stanReginCd.isArray() && stanReginCd.size() > 1) {
                    items = stanReginCd.get(1).path("row");
                    logger.info("🏛️ Found items in StanReginCd array structure: {}", items.size());
                } else if (stanReginCd.has("row")) {
                    items = stanReginCd.path("row");
                    logger.info("🏛️ Found items in StanReginCd row structure: {}", items.size());
                }
            }

            if (items != null && items.isArray() && items.size() > 0) {
                logger.info("🏛️ Processing {} potential regions from Government API", items.size());

                for (JsonNode item : items) {
                    String regionName = "";

                    // Try multiple field names for region information
                    String[] fieldNames = {"locatadr_nm", "region_nm", "sigun_nm", "administ_zone_nm", "bjdong_nm"};

                    for (String fieldName : fieldNames) {
                        regionName = item.path(fieldName).asText();
                        if (!regionName.isEmpty()) {
                            logger.debug("📍 Found region using field '{}': '{}'", fieldName, regionName);
                            break;
                        }
                    }

                    if (!regionName.isEmpty()) {
                        // Extract meaningful region parts (시, 구, 군)
                        String[] parts = regionName.split(" ");
                        for (String part : parts) {
                            if (part.contains("구") || part.contains("시") || part.contains("군")) {
                                regions.add(part.trim());
                                logger.debug("✅ Added region: '{}'", part.trim());
                                break;
                            }
                        }
                    }
                }
            } else {
                logger.warn("⚠️ No valid items array found in Government API response");
                logger.debug("🔍 Response keys: {}", root.fieldNames());
            }

        } catch (Exception e) {
            logger.error("❌ Failed to parse government API response: {}", e.getMessage(), e);
        }

        logger.info("🏛️ Successfully parsed {} regions from Government API", regions.size());
        return regions;
    }

    private List<String> getDefaultRegions() {
        return List.of("강남구", "마포구", "종로구", "중구", "용산구", "성동구", "광진구", "서대문구");
    }

    private void fetchPlacesFromNaver(String query) {
        try {
            // Use dynamic start position based on search offset
            int startPos = (searchOffset.get() % 5) * 10 + 1;

            String response = webClient.get()
                    .uri("https://openapi.naver.com/v1/search/local.json?query={query}&display=5&start={start}&sort=random",
                         query, startPos)
                    .header("X-Naver-Client-Id", naverClientId)
                    .header("X-Naver-Client-Secret", naverClientSecret)
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(10))
                    .block();

            if (response != null) {
                parseNaverResponse(response);
                logger.debug("📍 Fetched places for query: {} (start: {})", query, startPos);
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
                String title = item.path("title").asText();
                String category = item.path("category").asText();
                String address = item.path("address").asText();

                // Filter out hotels and accommodations
                if (isAccommodation(title, category)) {
                    logger.debug("🚫 Filtering out accommodation: {} (category: {})", title, category);
                    continue;
                }

                // Check for duplicates using title + address as unique key
                String uniqueKey = title + "|" + address;
                boolean isDuplicate = fetchedPlaces.stream()
                    .anyMatch(place -> {
                        String existingKey = place.getNaverPlace().getCleanTitle() + "|" + place.getNaverPlace().getAddress();
                        return existingKey.equals(uniqueKey);
                    });

                if (isDuplicate) {
                    logger.debug("🔄 Skipping duplicate place: {}", title);
                    continue;
                }

                NaverPlaceItem naverPlace = new NaverPlaceItem();
                naverPlace.setTitle(title);
                naverPlace.setCategory(category);
                naverPlace.setAddress(address);
                naverPlace.setRoadAddress(item.path("roadAddress").asText());
                naverPlace.setDescription(item.path("description").asText());
                naverPlace.setTelephone(item.path("telephone").asText());
                naverPlace.setMapx(item.path("mapx").asText());
                naverPlace.setMapy(item.path("mapy").asText());

                // Convert to EnrichedPlace (without Google data for now)
                EnrichedPlace enrichedPlace = new EnrichedPlace(naverPlace, null);
                fetchedPlaces.add(enrichedPlace);
                logger.debug("✅ Added unique place: {} (total: {})", title, fetchedPlaces.size());
            }

        } catch (Exception e) {
            logger.error("❌ Failed to parse Naver API response", e);
        }
    }

    /**
     * Check if a place is an accommodation (hotel, motel, pension, etc.)
     */
    private boolean isAccommodation(String title, String category) {
        // Check category for accommodation keywords
        if (category != null) {
            String categoryLower = category.toLowerCase();
            if (categoryLower.contains("숙박") || categoryLower.contains("호텔") ||
                categoryLower.contains("모텔") || categoryLower.contains("펜션") ||
                categoryLower.contains("리조트") || categoryLower.contains("게스트하우스")) {
                return true;
            }
        }

        // Check title for accommodation keywords
        if (title != null) {
            String titleLower = title.toLowerCase();
            if (titleLower.contains("호텔") || titleLower.contains("모텔") ||
                titleLower.contains("펜션") || titleLower.contains("리조트") ||
                titleLower.contains("게스트하우스") || titleLower.contains("여관")) {
                return true;
            }
        }

        return false;
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
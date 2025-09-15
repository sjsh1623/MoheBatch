package com.example.ingestion.batch.reader;

import com.example.ingestion.dto.GooglePlaceDetail;
import com.example.ingestion.dto.NaverPlaceItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock place reader for testing the optimized batch processing
 */
@Component
public class MockPlaceReader implements ItemReader<EnrichedPlace> {

    private static final Logger logger = LoggerFactory.getLogger(MockPlaceReader.class);

    private final List<EnrichedPlace> mockPlaces;
    private final AtomicInteger currentIndex = new AtomicInteger(0);

    public MockPlaceReader() {
        this.mockPlaces = createMockPlaces();
        logger.info("📋 Initialized MockPlaceReader with {} test places", mockPlaces.size());
    }

    @Override
    public EnrichedPlace read() {
        int index = currentIndex.getAndIncrement();

        if (index >= mockPlaces.size()) {
            logger.info("✅ MockPlaceReader finished - read {} places", index);
            return null; // End of data
        }

        EnrichedPlace place = mockPlaces.get(index);
        logger.debug("📖 Reading place #{}: {}", index + 1, place.getNaverPlace().getCleanTitle());

        return place;
    }

    private List<EnrichedPlace> createMockPlaces() {
        List<EnrichedPlace> places = new ArrayList<>();

        // Valid places (should pass filtering)
        places.add(createMockPlace("스타벅스 강남점", "카페", "서울시 강남구", 37.5665, 126.9780, 4.2, 150));
        places.add(createMockPlace("이디야커피 홍대점", "카페", "서울시 마포구", 37.5502, 126.9224, 4.0, 89));
        places.add(createMockPlace("파스쿠찌 명동점", "카페", "서울시 중구", 37.5636, 126.9748, 4.1, 76));
        places.add(createMockPlace("투썸플레이스 건대점", "카페", "서울시 광진구", 37.5465, 127.0949, 3.9, 102));

        // Places that should be filtered out
        places.add(createMockPlace("강남 클럽 오션", "클럽", "서울시 강남구", 37.5172, 127.0473, 3.5, 45));
        places.add(createMockPlace("이마트 트레이더스", "마트", "서울시 강남구", 37.5219, 126.9895, 3.8, 200));
        places.add(createMockPlace("세븐일레븐 홍대점", "편의점", "서울시 마포구", 37.5440, 127.0557, 3.2, 12));
        places.add(createMockPlace("나이트클럽 엘리트", "나이트클럽", "서울시 강남구", 37.5814, 127.0097, 3.3, 67));

        // More valid places
        places.add(createMockPlace("국립중앙박물관", "박물관", "서울시 용산구", 37.5267, 126.8956, 4.5, 1200));
        places.add(createMockPlace("롯데월드타워", "관광명소", "서울시 송파구", 37.5126, 127.1025, 4.3, 3400));

        logger.info("🏗️ Created {} mock places - {} should pass filtering, {} should be filtered out",
                   places.size(),
                   places.size() - 4, // 4 places should be filtered
                   4);

        return places;
    }

    private EnrichedPlace createMockPlace(String name, String category, String address,
                                        double lat, double lng, double rating, int reviewCount) {
        // Create Naver place
        NaverPlaceItem naverPlace = new NaverPlaceItem();
        naverPlace.setTitle(name);
        naverPlace.setCategory(category);
        naverPlace.setAddress(address);
        naverPlace.setDescription(String.format("%s은/는 %s에 위치한 %s입니다.", name, address, category));
        naverPlace.setMapx(String.valueOf(Math.round(lng * 10000000)));
        naverPlace.setMapy(String.valueOf(Math.round(lat * 10000000)));

        // Create Google place (some places might not have Google data)
        GooglePlaceDetail googlePlace = null;
        if (Math.random() > 0.3) { // 70% chance of having Google data
            googlePlace = new GooglePlaceDetail();
            googlePlace.setPlaceId("google_" + name.replaceAll("\\s+", "_"));
            googlePlace.setName(name);
            googlePlace.setFormattedAddress(address);
            googlePlace.setRating(rating);
            googlePlace.setUserRatingsTotal(reviewCount);

            // Set price level randomly
            if (Math.random() > 0.5) {
                googlePlace.setPriceLevel((int) (Math.random() * 4) + 1);
            }

            // Set types based on category
            List<String> types = new ArrayList<>();
            if (category.contains("카페")) {
                types.add("cafe");
                types.add("food");
            } else if (category.contains("클럽")) {
                types.add("night_club");
                types.add("bar");
            } else if (category.contains("마트")) {
                types.add("supermarket");
                types.add("store");
            } else if (category.contains("편의점")) {
                types.add("convenience_store");
            } else if (category.contains("박물관")) {
                types.add("museum");
                types.add("tourist_attraction");
            }
            googlePlace.setTypes(types);
        }

        return new EnrichedPlace(naverPlace, googlePlace);
    }

    /**
     * Reset reader for multiple runs
     */
    public void reset() {
        currentIndex.set(0);
        logger.info("🔄 MockPlaceReader reset - ready to read {} places again", mockPlaces.size());
    }

    public int getTotalPlaces() {
        return mockPlaces.size();
    }
}
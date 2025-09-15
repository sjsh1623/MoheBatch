package com.example.ingestion.service;

import com.example.ingestion.dto.ProcessedPlaceJava;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Service for filtering places based on business requirements
 * Excludes: 클럽, 나이트, 성인, 룸살롱, 마트, 편의점, 대형 유통점
 */
@Service
public class PlaceFilterService {

    private static final Logger logger = LoggerFactory.getLogger(PlaceFilterService.class);

    // Comprehensive list of excluded keywords (Korean)
    private static final Set<String> EXCLUDED_KEYWORDS = Set.of(
            // Adult entertainment
            "클럽", "나이트", "성인", "룸살롱", "룸", "살롱", "유흥", "단란주점", "호스트바", "노래방",
            "안마", "마사지", "스파", "에스테틱", "테라피",

            // Large retail stores
            "마트", "편의점", "대형마트", "할인점", "슈퍼마켓", "하이퍼마켓", "창고형할인점",
            "백화점", "아울렛", "쇼핑센터", "대형유통점", "도매점", "대형매장",

            // Additional exclusions for clarity
            "성인용품", "성인전용", "19금", "성인오락", "게임방", "피씨방", "인터넷카페",
            "코인노래방", "DVD방", "찜질방", "사우나", "목욕탕",

            // Chain convenience stores
            "CU", "GS25", "세븐일레븐", "7-Eleven", "미니스톱", "이마트24", "위드미",

            // Large supermarket chains
            "이마트", "롯데마트", "홈플러스", "하나로마트", "농협", "코스트코", "Costco"
    );

    // Additional category-based filters
    private static final Set<String> EXCLUDED_CATEGORIES = Set.of(
            "convenience_store", "supermarket", "department_store", "shopping_mall",
            "night_club", "bar", "casino", "adult_entertainment", "spa", "massage",
            "gas_station", "parking", "atm", "bank", "hospital", "pharmacy"
    );

    /**
     * Check if a place should be excluded based on filtering criteria
     * @param place Place to check
     * @return true if place should be excluded
     */
    public boolean shouldExcludePlace(ProcessedPlaceJava place) {
        try {
            if (place == null) {
                logger.warn("Place is null, excluding");
                return true;
            }

            // Check place name
            if (containsExcludedKeyword(place.getName())) {
                logger.debug("Excluding place '{}' - name contains excluded keyword", place.getName());
                return true;
            }

            // Check category
            if (containsExcludedKeyword(place.getCategory())) {
                logger.debug("Excluding place '{}' - category '{}' contains excluded keyword",
                           place.getName(), place.getCategory());
                return true;
            }

            // Check description
            if (place.getDescription() != null && containsExcludedKeyword(place.getDescription())) {
                logger.debug("Excluding place '{}' - description contains excluded keyword", place.getName());
                return true;
            }

            // Check types (Google Place types)
            if (place.getTypes() != null) {
                for (String type : place.getTypes()) {
                    if (EXCLUDED_CATEGORIES.contains(type.toLowerCase())) {
                        logger.debug("Excluding place '{}' - type '{}' is excluded", place.getName(), type);
                        return true;
                    }
                }
            }

            // Check address for chain store patterns
            if (place.getAddress() != null && containsChainStorePattern(place.getAddress())) {
                logger.debug("Excluding place '{}' - address contains chain store pattern", place.getName());
                return true;
            }

            return false;

        } catch (Exception e) {
            logger.error("Error checking place filter for '{}': {}",
                        place != null ? place.getName() : "null", e.getMessage());
            return true; // Exclude on error to be safe
        }
    }

    /**
     * Filter a list of places, removing excluded ones
     * @param places List of places to filter
     * @return Filtered list of places
     */
    public List<ProcessedPlaceJava> filterPlaces(List<ProcessedPlaceJava> places) {
        if (places == null || places.isEmpty()) {
            return List.of();
        }

        int originalCount = places.size();
        List<ProcessedPlaceJava> filtered = places.stream()
                .filter(place -> !shouldExcludePlace(place))
                .collect(Collectors.toList());

        int filteredCount = filtered.size();
        int excludedCount = originalCount - filteredCount;

        if (excludedCount > 0) {
            logger.info("Filtered out {} places from {}, {} remaining",
                       excludedCount, originalCount, filteredCount);
        }

        return filtered;
    }

    /**
     * Check if text contains any excluded keywords
     */
    private boolean containsExcludedKeyword(String text) {
        if (text == null || text.trim().isEmpty()) {
            return false;
        }

        String lowerText = text.toLowerCase();

        return EXCLUDED_KEYWORDS.stream()
                .anyMatch(keyword -> lowerText.contains(keyword.toLowerCase()));
    }

    /**
     * Check for chain store patterns in address
     */
    private boolean containsChainStorePattern(String address) {
        if (address == null || address.trim().isEmpty()) {
            return false;
        }

        String lowerAddress = address.toLowerCase();

        // Common chain store address patterns
        List<String> chainPatterns = Arrays.asList(
                "점", "지점", "본점", "분점", "매장", "센터점", "역점", "터미널점",
                "아울렛", "플라자", "타워", "빌딩점"
        );

        return chainPatterns.stream()
                .anyMatch(pattern -> lowerAddress.contains(pattern));
    }

    /**
     * Get statistics about filtering
     */
    public FilteringStats getFilteringStats(List<ProcessedPlaceJava> originalPlaces) {
        if (originalPlaces == null || originalPlaces.isEmpty()) {
            return new FilteringStats(0, 0, 0);
        }

        int total = originalPlaces.size();
        int excluded = (int) originalPlaces.stream()
                .mapToInt(place -> shouldExcludePlace(place) ? 1 : 0)
                .sum();
        int included = total - excluded;

        return new FilteringStats(total, included, excluded);
    }

    /**
     * Statistics class for filtering results
     */
    public static class FilteringStats {
        private final int total;
        private final int included;
        private final int excluded;

        public FilteringStats(int total, int included, int excluded) {
            this.total = total;
            this.included = included;
            this.excluded = excluded;
        }

        public int getTotal() { return total; }
        public int getIncluded() { return included; }
        public int getExcluded() { return excluded; }
        public double getExclusionRate() { return total > 0 ? (double) excluded / total * 100 : 0; }

        @Override
        public String toString() {
            return String.format("FilteringStats{total=%d, included=%d, excluded=%d, exclusionRate=%.1f%%}",
                               total, included, excluded, getExclusionRate());
        }
    }
}
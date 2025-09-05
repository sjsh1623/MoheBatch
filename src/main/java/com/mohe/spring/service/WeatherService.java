package com.mohe.spring.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Weather data provider interface for pluggable weather services
 */
interface WeatherProvider {
    WeatherData getCurrentWeather(double lat, double lon);
}

/**
 * Normalized weather data structure
 */
class WeatherData {
    private final double tempC;
    private final double tempF;
    private final String conditionCode; // "clear", "cloudy", "rain", "snow", etc.
    private final String conditionText;
    private final int humidity;
    private final double windSpeedKmh;
    private final String daypart; // "morning", "afternoon", "evening", "night"
    private final LocalDateTime timestamp;
    
    public WeatherData(double tempC, double tempF, String conditionCode, String conditionText,
                       int humidity, double windSpeedKmh, String daypart) {
        this.tempC = tempC;
        this.tempF = tempF;
        this.conditionCode = conditionCode;
        this.conditionText = conditionText;
        this.humidity = humidity;
        this.windSpeedKmh = windSpeedKmh;
        this.daypart = daypart;
        this.timestamp = LocalDateTime.now();
    }
    
    // Getters
    public double getTempC() { return tempC; }
    public double getTempF() { return tempF; }
    public String getConditionCode() { return conditionCode; }
    public String getConditionText() { return conditionText; }
    public int getHumidity() { return humidity; }
    public double getWindSpeedKmh() { return windSpeedKmh; }
    public String getDaypart() { return daypart; }
    public LocalDateTime getTimestamp() { return timestamp; }
}

/**
 * Weather context for recommendations
 */
class WeatherContext {
    private final WeatherData weather;
    private final boolean isRainy;
    private final boolean isCold;
    private final boolean isHot;
    private final boolean isComfortable;
    private final List<String> recommendedActivities;
    
    public WeatherContext(WeatherData weather, boolean isRainy, boolean isCold, 
                         boolean isHot, boolean isComfortable, List<String> recommendedActivities) {
        this.weather = weather;
        this.isRainy = isRainy;
        this.isCold = isCold;
        this.isHot = isHot;
        this.isComfortable = isComfortable;
        this.recommendedActivities = new ArrayList<>(recommendedActivities);
    }
    
    public static WeatherContext from(WeatherData weather) {
        boolean isRainy = weather.getConditionCode().toLowerCase().contains("rain") || 
                         weather.getConditionCode().toLowerCase().contains("shower");
        boolean isCold = weather.getTempC() < 10;
        boolean isHot = weather.getTempC() > 28;
        boolean isComfortable = weather.getTempC() >= 18.0 && weather.getTempC() <= 25.0;
        
        List<String> recommendedActivities = new ArrayList<>();
        if (isRainy) {
            recommendedActivities.addAll(Arrays.asList("indoor", "cafe", "museum", "shopping"));
        } else if (isCold) {
            recommendedActivities.addAll(Arrays.asList("indoor", "warm_place", "hot_drink"));
        } else if (isHot) {
            recommendedActivities.addAll(Arrays.asList("air_conditioned", "cold_drink", "indoor"));
        } else if (isComfortable) {
            recommendedActivities.addAll(Arrays.asList("outdoor", "walking", "park"));
        }
        
        return new WeatherContext(weather, isRainy, isCold, isHot, isComfortable, recommendedActivities);
    }
    
    // Getters
    public WeatherData getWeather() { return weather; }
    public boolean isRainy() { return isRainy; }
    public boolean isCold() { return isCold; }
    public boolean isHot() { return isHot; }
    public boolean isComfortable() { return isComfortable; }
    public List<String> getRecommendedActivities() { return new ArrayList<>(recommendedActivities); }
}

/**
 * OpenWeatherMap implementation of WeatherProvider
 */
@Service
class OpenWeatherMapProvider implements WeatherProvider {
    
    private static final Logger logger = LoggerFactory.getLogger(OpenWeatherMapProvider.class);
    private static final String BASE_URL = "https://api.openweathermap.org/data/2.5";
    
    private final WebClient webClient;
    private final String apiKey;
    
    public OpenWeatherMapProvider(WebClient webClient, 
                                 @Value("${weather.openweathermap.api-key:}") String apiKey) {
        this.webClient = webClient;
        this.apiKey = apiKey;
    }
    
    @Override
    public WeatherData getCurrentWeather(double lat, double lon) {
        if (apiKey == null || apiKey.isBlank()) {
            logger.warn("OpenWeatherMap API key not configured, returning mock data");
            return createMockWeatherData(lat, lon);
        }
        
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> response = webClient.get()
                .uri(BASE_URL + "/weather?lat=" + lat + "&lon=" + lon + "&appid=" + apiKey + "&units=metric")
                .retrieve()
                .bodyToMono(Map.class)
                .retryWhen(
                    Retry.backoff(3, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(5))
                        .filter(throwable -> throwable instanceof WebClientResponseException)
                )
                .block(Duration.ofSeconds(10));
                
            if (response == null) {
                throw new RuntimeException("Empty response from OpenWeatherMap");
            }
            
            return parseOpenWeatherMapResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to fetch weather data from OpenWeatherMap for lat={}, lon={}", lat, lon, e);
            return createMockWeatherData(lat, lon);
        }
    }
    
    @SuppressWarnings("unchecked")
    private WeatherData parseOpenWeatherMapResponse(Map<String, Object> response) {
        Map<String, Object> main = (Map<String, Object>) response.get("main");
        List<Map<String, Object>> weather = (List<Map<String, Object>>) response.get("weather");
        Map<String, Object> firstWeather = weather.get(0);
        
        double tempC = ((Number) main.get("temp")).doubleValue();
        double tempF = tempC * 9 / 5 + 32;
        int humidity = ((Number) main.get("humidity")).intValue();
        
        Map<String, Object> wind = (Map<String, Object>) response.get("wind");
        double windSpeedKmh = wind != null && wind.get("speed") != null 
            ? ((Number) wind.get("speed")).doubleValue() * 3.6 
            : 0.0;
        
        String conditionCode = normalizeConditionCode((String) firstWeather.get("main"));
        String conditionText = (String) firstWeather.get("description");
        
        String daypart = determineDaypart();
        
        return new WeatherData(tempC, tempF, conditionCode, conditionText, humidity, windSpeedKmh, daypart);
    }
    
    private String normalizeConditionCode(String owmCondition) {
        switch (owmCondition.toLowerCase()) {
            case "clear": return "clear";
            case "clouds": return "cloudy";
            case "rain":
            case "drizzle": return "rain";
            case "snow": return "snow";
            case "thunderstorm": return "thunderstorm";
            case "mist":
            case "fog":
            case "haze": return "fog";
            default: return "unknown";
        }
    }
    
    private String determineDaypart() {
        int hour = LocalDateTime.now().getHour();
        if (hour >= 6 && hour <= 11) return "morning";
        if (hour >= 12 && hour <= 17) return "afternoon";
        if (hour >= 18 && hour <= 21) return "evening";
        return "night";
    }
    
    private WeatherData createMockWeatherData(double lat, double lon) {
        // Create reasonable mock data based on location and time
        LocalDateTime now = LocalDateTime.now();
        double tempC;
        switch (now.getMonthValue()) {
            case 12: case 1: case 2:
                tempC = 5.0 + (Math.random() * 10); // Winter: 5-15°C
                break;
            case 3: case 4: case 5:
                tempC = 15.0 + (Math.random() * 10); // Spring: 15-25°C
                break;
            case 6: case 7: case 8:
                tempC = 25.0 + (Math.random() * 10); // Summer: 25-35°C
                break;
            default:
                tempC = 10.0 + (Math.random() * 15); // Fall: 10-25°C
                break;
        }
        
        return new WeatherData(
            tempC,
            tempC * 9 / 5 + 32,
            "clear",
            "Clear sky (mock data)",
            60,
            10.0,
            determineDaypart()
        );
    }
}

/**
 * Weather service orchestrator with pluggable providers
 */
@Service
public class WeatherService {
    
    private static final Logger logger = LoggerFactory.getLogger(WeatherService.class);
    private static final Duration CACHE_TIMEOUT = Duration.ofMinutes(10);
    
    private final WeatherProvider weatherProvider;
    
    // Simple in-memory cache for 10 minutes
    private final Map<String, CacheEntry> weatherCache = new ConcurrentHashMap<>();
    
    public WeatherService(WeatherProvider weatherProvider) {
        this.weatherProvider = weatherProvider;
    }
    
    public WeatherData getCurrentWeather(double lat, double lon) {
        String cacheKey = lat + "_" + lon;
        CacheEntry cached = weatherCache.get(cacheKey);
        
        if (cached != null && Duration.between(cached.timestamp, LocalDateTime.now()).compareTo(CACHE_TIMEOUT) < 0) {
            logger.debug("Returning cached weather data for lat={}, lon={}", lat, lon);
            return cached.weatherData;
        }
        
        logger.info("Fetching fresh weather data for lat={}, lon={}", lat, lon);
        WeatherData weather = weatherProvider.getCurrentWeather(lat, lon);
        weatherCache.put(cacheKey, new CacheEntry(weather, LocalDateTime.now()));
        
        // Clean old cache entries
        cleanupCache();
        
        return weather;
    }
    
    public WeatherContext getWeatherContext(double lat, double lon) {
        WeatherData weather = getCurrentWeather(lat, lon);
        return WeatherContext.from(weather);
    }
    
    private void cleanupCache() {
        LocalDateTime now = LocalDateTime.now();
        List<String> expiredKeys = weatherCache.entrySet().stream()
                .filter(entry -> Duration.between(entry.getValue().timestamp, now).compareTo(CACHE_TIMEOUT) > 0)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        
        expiredKeys.forEach(weatherCache::remove);
    }
    
    private static class CacheEntry {
        final WeatherData weatherData;
        final LocalDateTime timestamp;
        
        CacheEntry(WeatherData weatherData, LocalDateTime timestamp) {
            this.weatherData = weatherData;
            this.timestamp = timestamp;
        }
    }
}
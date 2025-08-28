package com.mohe.spring.service

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.util.retry.Retry
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/**
 * Weather data provider interface for pluggable weather services
 */
interface WeatherProvider {
    fun getCurrentWeather(lat: Double, lon: Double): WeatherData
}

/**
 * Normalized weather data structure
 */
data class WeatherData(
    val tempC: Double,
    val tempF: Double,
    val conditionCode: String, // "clear", "cloudy", "rain", "snow", etc.
    val conditionText: String,
    val humidity: Int,
    val windSpeedKmh: Double,
    val daypart: String, // "morning", "afternoon", "evening", "night"
    val timestamp: LocalDateTime = LocalDateTime.now()
)

/**
 * Weather context for recommendations
 */
data class WeatherContext(
    val weather: WeatherData,
    val isRainy: Boolean,
    val isCold: Boolean,
    val isHot: Boolean,
    val isComfortable: Boolean,
    val recommendedActivities: List<String>
) {
    companion object {
        fun from(weather: WeatherData): WeatherContext {
            val isRainy = weather.conditionCode.contains("rain", ignoreCase = true) || 
                         weather.conditionCode.contains("shower", ignoreCase = true)
            val isCold = weather.tempC < 10
            val isHot = weather.tempC > 28
            val isComfortable = weather.tempC in 18.0..25.0
            
            val recommendedActivities = mutableListOf<String>()
            when {
                isRainy -> recommendedActivities.addAll(listOf("indoor", "cafe", "museum", "shopping"))
                isCold -> recommendedActivities.addAll(listOf("indoor", "warm_place", "hot_drink"))
                isHot -> recommendedActivities.addAll(listOf("air_conditioned", "cold_drink", "indoor"))
                isComfortable -> recommendedActivities.addAll(listOf("outdoor", "walking", "park"))
            }
            
            return WeatherContext(weather, isRainy, isCold, isHot, isComfortable, recommendedActivities)
        }
    }
}

/**
 * OpenWeatherMap implementation of WeatherProvider
 */
@Service
class OpenWeatherMapProvider(
    private val webClient: WebClient,
    @Value("\${weather.openweathermap.api-key:}") private val apiKey: String
) : WeatherProvider {
    
    private val logger = LoggerFactory.getLogger(OpenWeatherMapProvider::class.java)
    private val baseUrl = "https://api.openweathermap.org/data/2.5"
    
    override fun getCurrentWeather(lat: Double, lon: Double): WeatherData {
        if (apiKey.isBlank()) {
            logger.warn("OpenWeatherMap API key not configured, returning mock data")
            return createMockWeatherData(lat, lon)
        }
        
        return try {
            val response = webClient.get()
                .uri("$baseUrl/weather?lat=$lat&lon=$lon&appid=$apiKey&units=metric")
                .retrieve()
                .bodyToMono(Map::class.java)
                .retryWhen(
                    Retry.backoff(3, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(5))
                        .filter { it is WebClientResponseException }
                )
                .block(Duration.ofSeconds(10))
                ?: throw RuntimeException("Empty response from OpenWeatherMap")
            
            parseOpenWeatherMapResponse(response)
            
        } catch (e: Exception) {
            logger.error("Failed to fetch weather data from OpenWeatherMap for lat=$lat, lon=$lon", e)
            createMockWeatherData(lat, lon)
        }
    }
    
    @Suppress("UNCHECKED_CAST")
    private fun parseOpenWeatherMapResponse(response: Map<*, *>): WeatherData {
        val main = response["main"] as Map<String, Any>
        val weather = (response["weather"] as List<Map<String, Any>>).first()
        
        val tempC = (main["temp"] as Number).toDouble()
        val tempF = tempC * 9 / 5 + 32
        val humidity = (main["humidity"] as Number).toInt()
        
        val wind = response["wind"] as? Map<String, Any>
        val windSpeedKmh = (wind?.get("speed") as? Number)?.toDouble()?.times(3.6) ?: 0.0
        
        val conditionCode = normalizeConditionCode(weather["main"] as String)
        val conditionText = weather["description"] as String
        
        val daypart = determineDaypart()
        
        return WeatherData(
            tempC = tempC,
            tempF = tempF,
            conditionCode = conditionCode,
            conditionText = conditionText,
            humidity = humidity,
            windSpeedKmh = windSpeedKmh,
            daypart = daypart
        )
    }
    
    private fun normalizeConditionCode(owmCondition: String): String {
        return when (owmCondition.lowercase()) {
            "clear" -> "clear"
            "clouds" -> "cloudy"
            "rain", "drizzle" -> "rain"
            "snow" -> "snow"
            "thunderstorm" -> "thunderstorm"
            "mist", "fog", "haze" -> "fog"
            else -> "unknown"
        }
    }
    
    private fun determineDaypart(): String {
        val hour = LocalDateTime.now().hour
        return when (hour) {
            in 6..11 -> "morning"
            in 12..17 -> "afternoon"
            in 18..21 -> "evening"
            else -> "night"
        }
    }
    
    private fun createMockWeatherData(lat: Double, lon: Double): WeatherData {
        // Create reasonable mock data based on location and time
        val now = LocalDateTime.now()
        val tempC = when (now.monthValue) {
            12, 1, 2 -> 5.0 + (Math.random() * 10) // Winter: 5-15°C
            3, 4, 5 -> 15.0 + (Math.random() * 10) // Spring: 15-25°C
            6, 7, 8 -> 25.0 + (Math.random() * 10) // Summer: 25-35°C
            else -> 10.0 + (Math.random() * 15) // Fall: 10-25°C
        }
        
        return WeatherData(
            tempC = tempC,
            tempF = tempC * 9 / 5 + 32,
            conditionCode = "clear",
            conditionText = "Clear sky (mock data)",
            humidity = 60,
            windSpeedKmh = 10.0,
            daypart = determineDaypart()
        )
    }
}

/**
 * Weather service orchestrator with pluggable providers
 */
@Service
class WeatherService(
    private val weatherProvider: WeatherProvider
) {
    private val logger = LoggerFactory.getLogger(WeatherService::class.java)
    
    // Simple in-memory cache for 10 minutes
    private val weatherCache = mutableMapOf<String, Pair<WeatherData, LocalDateTime>>()
    private val cacheTimeout = Duration.ofMinutes(10)
    
    fun getCurrentWeather(lat: Double, lon: Double): WeatherData {
        val cacheKey = "${lat}_${lon}"
        val cached = weatherCache[cacheKey]
        
        if (cached != null && Duration.between(cached.second, LocalDateTime.now()) < cacheTimeout) {
            logger.debug("Returning cached weather data for lat=$lat, lon=$lon")
            return cached.first
        }
        
        logger.info("Fetching fresh weather data for lat=$lat, lon=$lon")
        val weather = weatherProvider.getCurrentWeather(lat, lon)
        weatherCache[cacheKey] = Pair(weather, LocalDateTime.now())
        
        // Clean old cache entries
        cleanupCache()
        
        return weather
    }
    
    fun getWeatherContext(lat: Double, lon: Double): WeatherContext {
        val weather = getCurrentWeather(lat, lon)
        return WeatherContext.from(weather)
    }
    
    private fun cleanupCache() {
        val now = LocalDateTime.now()
        val expiredKeys = weatherCache.filterValues { 
            Duration.between(it.second, now) > cacheTimeout 
        }.keys
        expiredKeys.forEach { weatherCache.remove(it) }
    }
}
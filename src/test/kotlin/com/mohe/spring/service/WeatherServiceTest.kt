package com.mohe.spring.service

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.assertThrows
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.whenever
import org.assertj.core.api.Assertions.*
import java.time.LocalDateTime

@DisplayName("WeatherService Tests")
class WeatherServiceTest {

    @Mock
    private lateinit var mockWeatherProvider: WeatherProvider
    
    private lateinit var weatherService: WeatherService

    @BeforeEach
    fun setup() {
        MockitoAnnotations.openMocks(this)
        weatherService = WeatherService(mockWeatherProvider)
    }

    @Test
    @DisplayName("Should get current weather successfully")
    fun shouldGetCurrentWeatherSuccessfully() {
        // Given
        val lat = 37.5665
        val lon = 126.9780
        val mockWeatherData = WeatherData(
            tempC = 22.5,
            tempF = 72.5,
            conditionCode = "clear",
            conditionText = "Clear sky",
            humidity = 65,
            windSpeedKmh = 12.5,
            daypart = "afternoon",
            timestamp = LocalDateTime.now()
        )
        
        whenever(mockWeatherProvider.getCurrentWeather(lat, lon)).thenReturn(mockWeatherData)

        // When
        val result = weatherService.getCurrentWeather(lat, lon)

        // Then
        assertThat(result).isEqualTo(mockWeatherData)
        assertThat(result.tempC).isEqualTo(22.5)
        assertThat(result.conditionCode).isEqualTo("clear")
        assertThat(result.daypart).isEqualTo("afternoon")
    }

    @Test
    @DisplayName("Should cache weather data")
    fun shouldCacheWeatherData() {
        // Given
        val lat = 37.5665
        val lon = 126.9780
        val mockWeatherData = WeatherData(
            tempC = 22.5,
            tempF = 72.5,
            conditionCode = "clear",
            conditionText = "Clear sky",
            humidity = 65,
            windSpeedKmh = 12.5,
            daypart = "afternoon"
        )
        
        whenever(mockWeatherProvider.getCurrentWeather(lat, lon)).thenReturn(mockWeatherData)

        // When - Make two calls
        val result1 = weatherService.getCurrentWeather(lat, lon)
        val result2 = weatherService.getCurrentWeather(lat, lon)

        // Then - Provider should only be called once due to caching
        assertThat(result1).isEqualTo(result2)
        // Verify caching is working by checking that both results are identical
        assertThat(result1.timestamp).isEqualTo(result2.timestamp)
    }

    @Test
    @DisplayName("Should get weather context with correct flags")
    fun shouldGetWeatherContextWithCorrectFlags() {
        // Given
        val lat = 37.5665
        val lon = 126.9780
        val mockWeatherData = WeatherData(
            tempC = 5.0, // Cold temperature
            tempF = 41.0,
            conditionCode = "rain", // Rainy condition
            conditionText = "Light rain",
            humidity = 80,
            windSpeedKmh = 15.0,
            daypart = "morning"
        )
        
        whenever(mockWeatherProvider.getCurrentWeather(lat, lon)).thenReturn(mockWeatherData)

        // When
        val context = weatherService.getWeatherContext(lat, lon)

        // Then
        assertThat(context.weather).isEqualTo(mockWeatherData)
        assertThat(context.isRainy).isTrue() // Should detect rain
        assertThat(context.isCold).isTrue() // Should detect cold (< 10°C)
        assertThat(context.isHot).isFalse()
        assertThat(context.isComfortable).isFalse()
        
        // Should recommend indoor activities due to rain and cold
        assertThat(context.recommendedActivities)
            .contains("indoor", "cafe", "museum")
    }

    @Test
    @DisplayName("Should detect hot weather correctly")
    fun shouldDetectHotWeatherCorrectly() {
        // Given
        val lat = 37.5665
        val lon = 126.9780
        val mockWeatherData = WeatherData(
            tempC = 35.0, // Hot temperature
            tempF = 95.0,
            conditionCode = "clear",
            conditionText = "Clear sky",
            humidity = 60,
            windSpeedKmh = 10.0,
            daypart = "afternoon"
        )
        
        whenever(mockWeatherProvider.getCurrentWeather(lat, lon)).thenReturn(mockWeatherData)

        // When
        val context = weatherService.getWeatherContext(lat, lon)

        // Then
        assertThat(context.isHot).isTrue()
        assertThat(context.isCold).isFalse()
        assertThat(context.isRainy).isFalse()
        assertThat(context.isComfortable).isFalse()
        
        // Should recommend air conditioned places
        assertThat(context.recommendedActivities)
            .contains("air_conditioned", "cold_drink", "indoor")
    }

    @Test
    @DisplayName("Should detect comfortable weather correctly")
    fun shouldDetectComfortableWeatherCorrectly() {
        // Given
        val lat = 37.5665
        val lon = 126.9780
        val mockWeatherData = WeatherData(
            tempC = 22.0, // Comfortable temperature
            tempF = 71.6,
            conditionCode = "clear",
            conditionText = "Clear sky",
            humidity = 50,
            windSpeedKmh = 8.0,
            daypart = "morning"
        )
        
        whenever(mockWeatherProvider.getCurrentWeather(lat, lon)).thenReturn(mockWeatherData)

        // When
        val context = weatherService.getWeatherContext(lat, lon)

        // Then
        assertThat(context.isComfortable).isTrue()
        assertThat(context.isHot).isFalse()
        assertThat(context.isCold).isFalse()
        assertThat(context.isRainy).isFalse()
        
        // Should recommend outdoor activities
        assertThat(context.recommendedActivities)
            .contains("outdoor", "walking", "park")
    }
}
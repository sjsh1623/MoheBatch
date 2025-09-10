package com.example.ingestion.batch.writer

import com.example.ingestion.entity.Place
import com.example.ingestion.repository.PlaceRepository
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import org.springframework.batch.item.Chunk
import kotlin.test.assertEquals

class PlaceImageWriterTest {

    private lateinit var placeRepository: PlaceRepository
    private lateinit var writer: PlaceImageWriter

    @BeforeEach
    fun setUp() {
        placeRepository = mock(PlaceRepository::class.java)
    }

    @Test
    fun `should save places when not in dry run mode`() {
        // Given
        writer = PlaceImageWriter(placeRepository, false)
        val places = listOf(
            Place(id = 1L, name = "Place 1", imageUrl = "/images/places/2025/09/1_20250909120000.png"),
            Place(id = 2L, name = "Place 2", imageUrl = "/images/places/2025/09/2_20250909120001.png")
        )
        val chunk = Chunk(places)

        // When
        writer.write(chunk)

        // Then
        verify(placeRepository).saveAll(places)
    }

    @Test
    fun `should not save places when in dry run mode`() {
        // Given
        writer = PlaceImageWriter(placeRepository, true)
        val places = listOf(
            Place(id = 1L, name = "Place 1", imageUrl = "/images/places/2025/09/1_20250909120000.png")
        )
        val chunk = Chunk(places)

        // When
        writer.write(chunk)

        // Then
        verify(placeRepository, never()).saveAll(any())
    }

    @Test
    fun `should handle empty chunk gracefully`() {
        // Given
        writer = PlaceImageWriter(placeRepository, false)
        val chunk = Chunk<Place>(emptyList())

        // When
        writer.write(chunk)

        // Then
        verify(placeRepository).saveAll(emptyList())
    }

    @Test
    fun `should only update imageUrl field`() {
        // Given
        writer = PlaceImageWriter(placeRepository, false)
        val originalPlace = Place(
            id = 1L,
            name = "Original Name",
            description = "Original Description",
            imageUrl = null
        )
        val updatedPlace = originalPlace.copy(imageUrl = "/images/places/2025/09/1_20250909120000.png")
        val chunk = Chunk(listOf(updatedPlace))

        // When
        writer.write(chunk)

        // Then
        verify(placeRepository).saveAll(argThat { places ->
            places.size == 1 &&
            places.first().id == 1L &&
            places.first().name == "Original Name" &&
            places.first().description == "Original Description" &&
            places.first().imageUrl == "/images/places/2025/09/1_20250909120000.png"
        })
    }
}
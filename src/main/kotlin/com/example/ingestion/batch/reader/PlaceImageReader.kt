package com.example.ingestion.batch.reader

import com.example.ingestion.entity.Place
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.database.JpaPagingItemReader
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

@Configuration
class PlaceImageReader {

    @Bean
    @StepScope
    fun placeImageItemReader(
        entityManagerFactory: EntityManagerFactory,
        @Value("#{jobParameters['since']}") since: String?,
        @Value("#{jobParameters['limit']}") limit: Long?
    ): JpaPagingItemReader<Place> {
        val queryParams = mutableMapOf<String, Any>()
        var queryString = "SELECT p FROM Place p WHERE p.imageUrl IS NULL OR p.imageUrl = ''"

        since?.let {
            val startDate = LocalDate.parse(it).atStartOfDay()
            queryString += " AND p.updatedAt >= :since"
            queryParams["since"] = startDate
        }

        val reader = JpaPagingItemReaderBuilder<Place>()
            .name("placeImageReader")
            .entityManagerFactory(entityManagerFactory)
            .queryString(queryString)
            .parameterValues(queryParams)
            .pageSize(10)
            .build()

        limit?.let { reader.setMaxItemCount(it.toInt()) }

        return reader
    }
}

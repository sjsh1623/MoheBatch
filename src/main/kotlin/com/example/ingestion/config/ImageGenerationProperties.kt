package com.example.ingestion.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "app.batch.image-generation")
data class ImageGenerationProperties(
    val cli: CliProperties = CliProperties(),
    val publish: PublishProperties = PublishProperties(),
    val retry: RetryProperties = RetryProperties(),
    var concurrency: Int = 4,
) {
    data class CliProperties(
        var command: String = "gemini",
        var argsTemplate: String = "image:generate -p \"{PROMPT}\" -o \"{OUTPUT}\"",
        var imageSize: String = "1024x1024"
    )

    data class PublishProperties(
        var rootDir: String = "",
        var basePath: String = "/images/places"
    )

    data class RetryProperties(
        var maxAttempts: Int = 5,
        var initialBackoffMs: Long = 30000,
        var maxBackoffMs: Long = 600000
    )
}

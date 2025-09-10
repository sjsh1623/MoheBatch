package com.example.ingestion.config

import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.ExchangeFilterFunction
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import java.time.Duration
import java.util.concurrent.TimeUnit

@Configuration
class WebConfiguration(
    @Value("\${app.external.api.base-url}") private val baseUrl: String,
    @Value("\${app.external.api.timeout:30}") private val timeoutSeconds: Long
) {

    private val logger = LoggerFactory.getLogger(WebConfiguration::class.java)

    @Bean
    fun webClient(): WebClient {
        logger.info("Configuring WebClient with base URL: $baseUrl, timeout: ${timeoutSeconds}s")
        
        val httpClient = HttpClient.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (timeoutSeconds * 1000).toInt())
            .responseTimeout(Duration.ofSeconds(timeoutSeconds))
            .doOnConnected { conn ->
                conn.addHandlerLast(ReadTimeoutHandler(timeoutSeconds, TimeUnit.SECONDS))
                    .addHandlerLast(WriteTimeoutHandler(timeoutSeconds, TimeUnit.SECONDS))
            }

        return WebClient.builder()
            .baseUrl(baseUrl)
            .clientConnector(ReactorClientHttpConnector(httpClient))
            .filter(logRequest())
            .filter(logResponse())
            .filter(errorHandler())
            .build()
    }

    @Bean("externalApiWebClient")
    fun externalApiWebClient(): WebClient {
        logger.info("Configuring external API WebClient without base URL, timeout: ${timeoutSeconds}s")
        
        val httpClient = HttpClient.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (timeoutSeconds * 1000).toInt())
            .responseTimeout(Duration.ofSeconds(timeoutSeconds))
            .doOnConnected { conn ->
                conn.addHandlerLast(ReadTimeoutHandler(timeoutSeconds, TimeUnit.SECONDS))
                    .addHandlerLast(WriteTimeoutHandler(timeoutSeconds, TimeUnit.SECONDS))
            }

        return WebClient.builder()
            .clientConnector(ReactorClientHttpConnector(httpClient))
            .codecs { configurer ->
                configurer.defaultCodecs().maxInMemorySize(1024 * 1024) // 1MB buffer for large government API responses
            }
            .filter(logRequest())
            .filter(logResponse())
            .filter(errorHandler())
            .build()
    }

    private fun logRequest(): ExchangeFilterFunction {
        return ExchangeFilterFunction.ofRequestProcessor { clientRequest ->
            logger.debug("API Request: {} {}", clientRequest.method(), clientRequest.url())
            clientRequest.headers().forEach { name, values ->
                if (!name.equals("authorization", ignoreCase = true)) {
                    logger.debug("Request Header: {}: {}", name, values)
                }
            }
            Mono.just(clientRequest)
        }
    }

    private fun logResponse(): ExchangeFilterFunction {
        return ExchangeFilterFunction.ofResponseProcessor { clientResponse ->
            logger.debug("API Response: {}", clientResponse.statusCode())
            clientResponse.headers().asHttpHeaders().forEach { name, values ->
                logger.debug("Response Header: {}: {}", name, values)
            }
            Mono.just(clientResponse)
        }
    }

    private fun errorHandler(): ExchangeFilterFunction {
        return ExchangeFilterFunction.ofResponseProcessor { clientResponse ->
            if (clientResponse.statusCode().isError) {
                logger.error("API Error Response: {}", clientResponse.statusCode())
            }
            Mono.just(clientResponse)
        }
    }
}
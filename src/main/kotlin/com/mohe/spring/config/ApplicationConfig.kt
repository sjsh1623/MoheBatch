package com.mohe.spring.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.transaction.annotation.EnableTransactionManagement
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration

@Configuration
@EnableJpaRepositories(basePackages = ["com.mohe.spring.repository"])
@EnableTransactionManagement
@EnableScheduling
class ApplicationConfig {
    
    @Bean
    fun webClient(): WebClient {
        return WebClient.builder()
            .build()
    }
}
package com.mohe.spring.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.client.RestTemplate;

@Configuration
@EnableJpaRepositories(basePackages = "com.mohe.spring.repository")
@EnableTransactionManagement
@EnableScheduling
public class ApplicationConfig {
    
    
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
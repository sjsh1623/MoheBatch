package com.mohe.spring.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.time.Duration;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Value("${IMAGE_STORAGE_DIR:/host/images}")
    private String imageStorageDir;

    @Bean
    public WebClient webClient() {
        return WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(1024 * 1024 * 10)) // 10MB
                .build();
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // Serve images from /image directory at localhost:8080/image/
        String resourceLocation = "file:" + imageStorageDir + "/";
        registry.addResourceHandler("/image/**")
                .addResourceLocations(resourceLocation);

        // Serve images from /images/places directory
        registry.addResourceHandler("/images/places/**")
                .addResourceLocations(resourceLocation);
    }
}

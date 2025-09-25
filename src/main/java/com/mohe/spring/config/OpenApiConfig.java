package com.mohe.spring.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI()
            .info(new Info()
                .title("MOHE Spring Boot API")
                .description("사용자의 성향과 선호도를 기반으로 한 장소 추천 REST API")
                .version("1.0.0")
                .contact(new Contact()
                    .name("MOHE Development Team")
                    .email("dev@mohe.app")
                )
            )
            .servers(List.of(
                new Server().url("http://localhost:8080").description("Local Development Server"),
                new Server().url("https://api.mohe.app").description("Production Server")
            ))
            .components(new Components()
                .addSecuritySchemes("bearerAuth", new SecurityScheme()
                    .type(SecurityScheme.Type.HTTP)
                    .scheme("bearer")
                    .bearerFormat("JWT")
                    .description("JWT 토큰을 사용한 인증. 헤더에 'Bearer {token}' 형식으로 전송")
                )
            )
            .addSecurityItem(new SecurityRequirement().addList("bearerAuth"));
    }
}
package com.mohe.spring.config

import io.swagger.v3.oas.models.Components
import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Contact
import io.swagger.v3.oas.models.info.Info
import io.swagger.v3.oas.models.security.SecurityRequirement
import io.swagger.v3.oas.models.security.SecurityScheme
import io.swagger.v3.oas.models.servers.Server
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OpenApiConfig {

    @Bean
    fun openAPI(): OpenAPI {
        return OpenAPI()
            .info(
                Info()
                    .title("MOHE Spring Boot API")
                    .description("사용자의 성향과 선호도를 기반으로 한 장소 추천 REST API")
                    .version("1.0.0")
                    .contact(
                        Contact()
                            .name("MOHE Development Team")
                            .email("dev@mohe.app")
                    )
            )
            .servers(
                listOf(
                    Server().url("http://localhost:8080").description("Local Development Server"),
                    Server().url("https://api.mohe.app").description("Production Server")
                )
            )
            .components(
                Components()
                    .addSecuritySchemes(
                        "bearerAuth",
                        SecurityScheme()
                            .type(SecurityScheme.Type.HTTP)
                            .scheme("bearer")
                            .bearerFormat("JWT")
                            .description("JWT 토큰을 사용한 인증. 헤더에 'Bearer {token}' 형식으로 전송")
                    )
            )
            .addSecurityItem(
                SecurityRequirement().addList("bearerAuth")
            )
    }
}
# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

### Gradle Tasks
```bash
# Build the application
./gradlew build

# Run tests (uses H2 in-memory database)
./gradlew test

# Run specific test class
./gradlew test --tests "com.mohe.spring.MoheSpringApplicationTests"

# Clean build
./gradlew clean build
```

### Docker Development
```bash
# Start PostgreSQL and Spring app
docker compose up --build

# Start only PostgreSQL for local development
docker compose up postgres

# Stop all services
docker compose down
```

### Application URLs
- **Health Check**: http://localhost:8080/health
- **Swagger UI**: http://localhost:8080/swagger-ui.html
- **OpenAPI Spec**: http://localhost:8080/v3/api-docs

## Architecture Overview

### Core Design Patterns

**API Response Pattern**: All controllers use a standardized `ApiResponse<out T>` wrapper with covariant generic type to handle Kotlin variance issues. The pattern is:
```kotlin
// Success response
ResponseEntity.ok(ApiResponse.success(data))

// Error response  
ResponseEntity.badRequest().body(ApiResponse.error(code, message, path))

// Unauthorized (not ResponseEntity.unauthorized() - doesn't exist)
ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(ApiResponse.error(...))
```

**Security Architecture**: JWT-based stateless authentication with:
- Access tokens (1 hour expiry) for API calls
- Refresh tokens (30 days expiry) for token renewal
- Spring Security filter chain with custom JWT authentication filter
- Role-based access control with `@PreAuthorize("hasRole('USER')")`

**Database Layer**: JPA/Hibernate with:
- PostgreSQL for production/docker
- H2 in-memory for tests (profile: `test`)
- HikariCP connection pooling
- Database initialization via `src/main/resources/db/init.sql`

### Package Structure

- **config/**: Spring configuration classes (Security, OpenAPI, Application)
- **controller/**: REST endpoints organized by domain (Auth, User, Place, Bookmark, Activity)
- **dto/**: Data transfer objects with validation annotations
- **entity/**: JPA entities representing database tables
- **repository/**: Spring Data JPA repositories
- **service/**: Business logic layer
- **security/**: JWT handling, user authentication, custom filters
- **exception/**: Global exception handler for consistent error responses

### Key Technical Details

**Swagger Integration**: Uses SpringDoc OpenAPI 3 with comprehensive Korean documentation. All controllers use `@SwaggerApiResponse` (aliased to avoid conflict with custom `ApiResponse` class).

**MBTI-Based Recommendations**: Core business logic includes MBTI personality type matching for place recommendations. The `places` table includes MBTI scoring fields, and the recommendation algorithm considers user preferences and personality type.

**Multi-Step Registration**: Authentication flow supports:
1. Email signup → OTP verification → nickname/password setup
2. Temporary user storage during registration process
3. Email verification with 5-digit OTP codes

**Profile Management**: Users can set comprehensive preferences including MBTI type, age range, transportation method, and space preferences (workshop, exhibition, nature, etc.).

## Important Implementation Notes

**Kotlin-Specific Considerations**: 
- The `ApiResponse<out T>` uses covariant generics to resolve variance issues with Spring's ResponseEntity
- Controller methods should not use explicit type parameters on ResponseEntity methods (they cause compilation errors)
- Source code is in `src/main/java/` directory despite being Kotlin (project structure preference)

**Database Connection**:
- Docker profile uses `postgres:5432` hostname
- Local profile uses `localhost:5432`
- Test profile automatically uses H2 in-memory database

**Environment Profiles**:
- `docker`: Containerized deployment
- `local`: Local development with external PostgreSQL  
- `test`: Automated testing with H2 database

**Security Configuration**: Public endpoints (no authentication required):
- `/api/auth/**` - Authentication endpoints
- `/health` - Health check
- `/swagger-ui/**` - API documentation
- `/v3/api-docs/**` - OpenAPI specification
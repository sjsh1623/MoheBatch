# MOHE Spring Boot API

A comprehensive Spring Boot application built with Kotlin, providing REST APIs for the MOHE place recommendation mobile application.

## 🚀 Features

### Authentication & Security
- ✅ JWT-based authentication with access/refresh tokens
- ✅ Email verification with OTP
- ✅ Password reset functionality
- ✅ Spring Security configuration
- ✅ Role-based access control

### Core Functionality
- ✅ User management and preferences
- ✅ Place recommendations based on MBTI and preferences
- ✅ Place search with contextual filters (weather, time)
- ✅ Bookmark system
- ✅ Recent view tracking
- ✅ Place details with comprehensive data

### Technical Stack
- **Framework**: Spring Boot 3.2.0
- **Language**: Kotlin 1.9.20
- **Database**: PostgreSQL with HikariCP connection pooling
- **Security**: Spring Security + JWT
- **Email**: Spring Mail
- **Testing**: JUnit 5 + H2 for tests
- **Build**: Gradle with Kotlin DSL

## 📁 Project Structure

```
src/main/kotlin/com/mohe/spring/
├── config/         # Spring configuration
├── controller/     # REST controllers
├── dto/           # Data transfer objects
├── entity/        # JPA entities
├── exception/     # Global exception handling
├── repository/    # Data access layer
├── security/      # JWT & authentication
└── service/       # Business logic
```

## 🔗 API Endpoints

### Authentication (`/api/auth`)
- `POST /login` - User login
- `POST /signup` - User registration 
- `POST /verify-email` - OTP verification
- `POST /check-nickname` - Nickname availability
- `POST /setup-password` - Complete registration
- `POST /refresh` - Token refresh
- `POST /logout` - User logout
- `POST /forgot-password` - Password reset request
- `POST /reset-password` - Reset password

### User Management (`/api/user`)
- `GET /profile` - Get user profile
- `PUT /profile` - Update profile
- `PUT /preferences` - Set user preferences
- `GET /recent-places` - Recent viewed places
- `GET /my-places` - User contributed places

### Places (`/api/places`)
- `GET /recommendations` - Personalized recommendations
- `GET /` - List places with pagination
- `GET /{id}` - Place details
- `GET /search` - Search places with filters

### Bookmarks (`/api/bookmarks`)
- `POST /toggle` - Add/remove bookmark
- `GET /` - Get user bookmarks

## 🐳 Docker Setup

### Database Schema
The application automatically initializes the PostgreSQL database with:
- User management tables
- Place data with comprehensive attributes
- Bookmark and activity tracking
- JWT token storage
- Email verification system

### Running with Docker

```bash
# Build and start services
docker compose up --build

# Services:
# - PostgreSQL: localhost:5432
# - Spring App: localhost:8080
# - Health Check: http://localhost:8080/health
# - Swagger UI: http://localhost:8080/swagger-ui.html
```

## 🧪 Testing

```bash
# Run tests
./gradlew test

# Build application  
./gradlew build
```

## 📊 Database Schema

### Key Tables:
- **users** - User accounts and preferences
- **places** - Location data with MBTI scoring
- **bookmarks** - User bookmarks
- **recent_views** - Activity tracking
- **refresh_tokens** - JWT token management
- **temp_users** - Registration workflow
- **place_mbti_score** - MBTI-based recommendations

## 📖 API Documentation

### Swagger UI
Interactive API documentation is available at:
- **Development**: http://localhost:8080/swagger-ui.html
- **OpenAPI JSON**: http://localhost:8080/v3/api-docs

### API Features
- ✅ **Complete API Documentation** with Korean descriptions
- ✅ **Interactive Testing** with Try It Out functionality
- ✅ **JWT Authentication** with Bearer token support
- ✅ **Request/Response Examples** for all endpoints
- ✅ **Parameter Validation** documentation
- ✅ **Error Response** examples with Korean messages

### API Categories
1. **인증 관리** - Authentication and user registration
2. **사용자 관리** - Profile and preferences management  
3. **장소 관리** - Place recommendations and search
4. **북마크 관리** - Bookmark functionality
5. **사용자 활동** - Recent views and user activity
6. **시스템** - Health checks and system status

## 🔧 Configuration

### Application Properties
- JWT secret and expiration settings
- HikariCP connection pool configuration
- PostgreSQL database connection
- Email service settings
- Logging configuration

### Environment Profiles
- `docker` - For containerized deployment
- `local` - For local development
- `test` - For testing with H2 database

## 🎯 API Documentation Features

The API implements the complete specification from the Korean documentation:

### 🔐 Authentication Flow
1. Email signup → OTP verification → Password setup
2. Login with JWT tokens (access + refresh)
3. Automatic token refresh and logout

### 👤 User Experience  
1. MBTI-based personality preferences
2. Age range and transportation preferences
3. Space type preferences (workshop, exhibition, nature, etc.)
4. Personalized place recommendations

### 📍 Place Discovery
1. Context-aware search (weather, time, location)
2. MBTI-matched recommendations
3. Rating and popularity-based sorting
4. Comprehensive place details with images

### 💾 Data Management
1. Bookmark system with toggle functionality
2. Recent view history tracking
3. User activity monitoring
4. Preference-based filtering

## 🚀 Next Steps

The application is production-ready and includes:
- ✅ Complete API implementation
- ✅ **Comprehensive Swagger documentation**
- ✅ **Interactive API testing** via Swagger UI
- ✅ Security best practices
- ✅ Database optimization
- ✅ Error handling
- ✅ Docker deployment
- ✅ Testing framework

### 📖 API Documentation Access
- **Swagger UI**: http://localhost:8080/swagger-ui.html
- **OpenAPI Spec**: http://localhost:8080/v3/api-docs
- **All endpoints** documented with Korean descriptions and examples

Ready for frontend integration and deployment!
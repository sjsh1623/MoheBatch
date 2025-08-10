# MoheBatch

A production-ready, Kotlin-based Spring Boot application that uses Spring Batch to ingest data from external REST APIs, process and validate it, and store it in PostgreSQL with full idempotency guarantees.

## 🚀 Features

### Core Functionality
- **Paginated API Ingestion**: Reads data from external REST APIs with configurable pagination
- **Data Validation & Normalization**: Validates and normalizes incoming data with comprehensive error handling
- **Idempotent Upserts**: Uses PostgreSQL `ON CONFLICT` to ensure data consistency and prevent duplicates
- **Chunk Processing**: Processes data in configurable chunks (default: 500 items) for optimal performance
- **Retry & Backoff**: Implements exponential backoff retry logic for transient failures
- **Skip Policy**: Configurable skip logic for invalid records with detailed logging
- **Job Resumability**: Tracks processing state to resume from the last successful page after failures

### Web API
- **RESTful Endpoints**: Complete CRUD operations for ingested data
- **Search & Filtering**: Search by email, department, status with pagination
- **Health Checks**: Comprehensive health and readiness endpoints
- **Manual Job Triggering**: API endpoint to trigger batch jobs on-demand
- **Data Summary**: Statistics and metrics about ingested data

### Observability
- **Structured Logging**: Correlation IDs, batch metrics, and detailed error logging
- **Prometheus Metrics**: Built-in metrics for monitoring batch performance
- **Health Monitoring**: Database connectivity and external API health checks
- **Job Execution Tracking**: Detailed job execution history and statistics

### Production Ready
- **Docker Support**: Multi-stage Dockerfile with optimized JVM settings
- **Docker Compose**: Complete environment with PostgreSQL and monitoring
- **Database Migrations**: Flyway-based schema management
- **Environment Configuration**: 12-factor app configuration with environment variables
- **Resource Management**: Connection pooling, memory optimization, and graceful shutdown

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         MoheBatch                              │
├─────────────────────────────────────────────────────────────────┤
│  Web Layer (Controllers)                                       │
│  ├─ DataController (CRUD, Search, Manual Triggers)             │
│  └─ HealthController (Health, Readiness, Liveness)             │
├─────────────────────────────────────────────────────────────────┤
│  Service Layer                                                  │
│  └─ DataService (Business Logic, Validation)                   │
├─────────────────────────────────────────────────────────────────┤
│  Spring Batch Components                                       │
│  ├─ ExternalApiReader (Paginated API consumption)              │
│  ├─ DataProcessor (Validation & Normalization)                 │
│  ├─ DatabaseWriter (Idempotent upserts)                        │
│  └─ JobExecutionListener (Monitoring & State tracking)         │
├─────────────────────────────────────────────────────────────────┤
│  Data Layer                                                     │
│  ├─ DataRepository (Main data operations)                      │
│  ├─ JobExecutionStateRepository (Job state tracking)           │
│  └─ PostgreSQL (Primary data store)                            │
└─────────────────────────────────────────────────────────────────┘
```

## 🛠️ Technology Stack

- **Framework**: Spring Boot 3.2.0 with Kotlin 1.9.20
- **Batch Processing**: Spring Batch with chunk-oriented processing
- **Database**: PostgreSQL with HikariCP connection pooling
- **HTTP Client**: Spring WebFlux WebClient with retry logic
- **Database Migration**: Flyway
- **Metrics**: Micrometer with Prometheus support
- **Testing**: JUnit 5, Mockito, TestContainers
- **Containerization**: Docker with multi-stage builds
- **Build Tool**: Gradle with Kotlin DSL

## 📦 Quick Start

### Prerequisites

- Java 17 or higher
- Docker and Docker Compose
- PostgreSQL (if running locally)

### 1. Clone and Build

```bash
git clone <repository-url>
cd MoheBatch

# Copy environment variables
cp .env.example .env

# Build the application
./gradlew build
```

### 2. Run with Docker Compose

The application runs as two separate services:
- **Web Service** (port 8080): REST API and web endpoints
- **Batch Service** (port 8082): Background data processing with scheduling

```bash
# Start all services (PostgreSQL + Web + Batch + Mock API)
docker-compose up --build

# Start individual services
docker-compose up mohebatch-web        # Web service only
docker-compose up mohebatch-processor  # Batch service only

# Start with monitoring stack
docker-compose --profile monitoring up --build

# Start only database for local development
docker-compose up postgres
```

### 3. Verify Installation

```bash
# Check web service health
curl http://localhost:8080/health

# Check batch service health  
curl http://localhost:8082/health

# Check web service readiness (includes database connectivity)
curl http://localhost:8081/actuator/health

# Check batch service readiness
curl http://localhost:8083/actuator/health

# View API documentation
open http://localhost:8080/swagger-ui.html

# Check metrics
curl http://localhost:8081/actuator/metrics  # Web service metrics
curl http://localhost:8083/actuator/metrics  # Batch service metrics
```

## 🔧 Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `jdbc:postgresql://localhost:5432/mohe_db` | PostgreSQL connection string (shared with MoheSpring) |
| `DATABASE_USERNAME` | `mohe_user` | Database username |
| `DATABASE_PASSWORD` | `mohe_password` | Database password |
| `WEB_PORT` | `8080` | Web service port |
| `BATCH_PORT` | `8082` | Batch service port |
| `WEB_MANAGEMENT_PORT` | `8081` | Web service management port |
| `BATCH_MANAGEMENT_PORT` | `8083` | Batch service management port |
| `EXTERNAL_API_BASE_URL` | `http://localhost:3000` | Base URL of external API |
| `EXTERNAL_API_PAGE_SIZE` | `100` | Number of items per API page |
| `EXTERNAL_API_MAX_PAGES` | `100` | Maximum pages to process |
| `EXTERNAL_API_TIMEOUT` | `30` | API request timeout (seconds) |
| `BATCH_CHUNK_SIZE` | `500` | Items processed per chunk |
| `BATCH_SKIP_LIMIT` | `100` | Maximum items to skip before failing |
| `BATCH_JOB_NAME` | `data-ingestion-job` | Spring Batch job name |
| `SPRING_PROFILES_ACTIVE` | `local` | Active Spring profile |
| `LOG_LEVEL` | `INFO` | Application log level |

### Database Configuration

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 20000
      idle-timeout: 300000
      max-lifetime: 1200000
      leak-detection-threshold: 60000
```

### Batch Configuration

```yaml
app:
  batch:
    chunk-size: 500
    skip-limit: 100
  external:
    api:
      page-size: 100
      max-pages: 100
      timeout: 30
```

## 🔄 Running Batch Jobs

### Manual Trigger via API

```bash
# Trigger batch job via web service
curl -X POST http://localhost:8080/api/data/batch/trigger

# Trigger batch job directly on batch service
curl -X POST http://localhost:8082/api/data/batch/trigger

# Force restart from page 1
curl -X POST "http://localhost:8080/api/data/batch/trigger?forceRestart=true"
```

### Command Line Execution

```bash
# Run specific job with parameters
java -jar app.jar --job.name=dataIngestionJob --force.restart=true
```

### Scheduled Execution

#### Scheduled Batch Processing

The batch service automatically runs on a configurable schedule (default: every 30 minutes).

##### Cron (Linux/Mac)
```bash
# Add to crontab for daily execution at 2 AM (web service)
0 2 * * * curl -X POST http://localhost:8080/api/data/batch/trigger

# Or trigger batch service directly
0 2 * * * curl -X POST http://localhost:8082/api/data/batch/trigger
```

#### Kubernetes CronJob
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: data-ingestion-job
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: batch-trigger
            image: curlimages/curl:latest
            command:
            - /bin/sh
            - -c
            - "curl -X POST http://mohebatch:8080/api/data/batch/trigger"
          restartPolicy: OnFailure
```

#### AWS ECS Scheduled Tasks
```json
{
  "family": "mohebatch-trigger",
  "taskRoleArn": "arn:aws:iam::account:role/ecsTaskRole",
  "networkConfiguration": {
    "awsvpcConfiguration": {
      "subnets": ["subnet-12345"],
      "securityGroups": ["sg-12345"],
      "assignPublicIp": "ENABLED"
    }
  },
  "containerDefinitions": [{
    "name": "batch-trigger",
    "image": "curlimages/curl:latest",
    "command": ["sh", "-c", "curl -X POST http://service-url:8080/api/data/batch/trigger"]
  }]
}
```

## 📊 API Endpoints

### Data Management
- `GET /api/data` - List all data with pagination
- `GET /api/data/{id}` - Get data by internal ID
- `GET /api/data/external/{externalId}` - Get data by external ID
- `POST /api/data` - Create or update data
- `GET /api/data/search/email?email={email}` - Search by email
- `GET /api/data/search/department?department={dept}` - Search by department
- `GET /api/data/status/{status}` - Filter by status
- `GET /api/data/summary` - Get data statistics
- `GET /api/data/updated-after?timestamp={iso-datetime}` - Get recent updates

### Batch Operations
- `POST /api/data/batch/trigger` - Trigger batch job manually
- `POST /api/data/batch/trigger?forceRestart=true` - Force restart from page 1

### Health & Monitoring
- `GET /health` - Basic health check
- `GET /health/ready` - Readiness check (includes dependencies)
- `GET /health/liveness` - Liveness check
- `GET /actuator/health` - Detailed health information
- `GET /actuator/metrics` - Application metrics
- `GET /actuator/prometheus` - Prometheus metrics endpoint

## 🔍 Idempotency & Data Consistency

### Upsert Strategy
The application uses PostgreSQL's `ON CONFLICT` clause to ensure idempotency:

```sql
INSERT INTO ingested_data (external_id, name, email, ...)
VALUES (?, ?, ?, ...)
ON CONFLICT (external_id) DO UPDATE SET
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    updated_at = EXCLUDED.updated_at
```

### Timestamp-Based Updates
- Records are only updated if the external `updated_at` timestamp is newer
- Original creation timestamps are preserved
- Prevents data regression from stale API responses

### Job Resumability
- Job execution state is tracked in the `job_execution_state` table
- Failed jobs can resume from the last successful page
- Prevents duplicate processing of already-ingested pages

## 🔁 Retry Logic & Error Handling

### HTTP Client Retries
- **Exponential backoff**: 1s → 2s → 4s → 8s (configurable)
- **Retry conditions**: 5xx server errors, network timeouts
- **Circuit breaker**: Fails fast after consecutive failures

### Batch Processing Retries
- **Item-level retries**: 3 attempts for transient errors
- **Skip policy**: Configurable skip limits for invalid data
- **Chunk-level rollback**: Failed chunks are retried as individual items

### Error Categories

| Error Type | Action | Skip? |
|------------|--------|-------|
| Validation errors | Log and skip | ✅ |
| Data format errors | Log and skip | ✅ |
| Network timeouts | Retry with backoff | ❌ |
| Database connectivity | Fail job | ❌ |
| External API 5xx | Retry with backoff | ❌ |
| External API 4xx | Log and skip | ✅ |

## 🧪 Testing

### Run Tests

```bash
# Run all tests
./gradlew test

# Run specific test class
./gradlew test --tests "DataProcessorTest"

# Run with coverage
./gradlew test jacocoTestReport
```

### Test Categories

- **Unit Tests**: Business logic, validation, data processing
- **Integration Tests**: Database operations, batch job execution
- **Contract Tests**: External API interactions with WireMock

### Test Configuration

Tests use H2 in-memory database and WireMock for external API simulation:

```yaml
spring:
  profiles:
    active: test
  datasource:
    url: jdbc:h2:mem:testdb
  batch:
    initialize-schema: embedded
```

## 📈 Monitoring & Observability

### Metrics Available

- **Batch Job Metrics**:
  - `batch_job_duration` - Job execution time
  - `batch_items_processed` - Successfully processed items
  - `batch_items_skipped` - Skipped invalid items
  - `batch_database_inserts` - New records inserted
  - `batch_database_updates` - Existing records updated

- **API Metrics**:
  - `external_api_requests_success` - Successful API calls
  - `external_api_requests_error` - Failed API calls
  - `api_errors` - Web API error counts by type

- **System Metrics**:
  - JVM memory, GC, thread pools
  - Database connection pool statistics
  - HTTP request/response metrics

### Structured Logging

All logs include correlation IDs and structured fields:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "thread": "batch-thread-1",
  "logger": "ExternalApiReader",
  "correlationId": "batch-1705317000000-123",
  "message": "Successfully fetched page 5 with 100 items",
  "page": 5,
  "itemCount": 100,
  "duration": 1250
}
```

### Grafana Dashboard

Example queries for monitoring:

```promql
# Job success rate
rate(batch_job_completed_total[5m]) / rate(batch_job_total[5m]) * 100

# Processing throughput
rate(batch_items_processed_total[5m])

# API error rate
rate(external_api_requests_error_total[5m]) / rate(external_api_requests_total[5m]) * 100

# Database connection usage
hikaricp_connections_active / hikaricp_connections_max * 100
```

## 🚀 Production Deployment

### Docker

```bash
# Build production image
docker build -t mohebatch:latest .

# Run web service with production settings
docker run -d \
  --name mohebatch-web \
  -p 8080:8080 \
  -p 8081:8081 \
  -e DATABASE_URL=jdbc:postgresql://prod-db:5432/mohe_db \
  -e EXTERNAL_API_BASE_URL=https://api.production.com \
  -e SPRING_PROFILES_ACTIVE=prod \
  mohebatch:latest

# Run batch service with production settings
docker run -d \
  --name mohebatch-processor \
  -p 8082:8080 \
  -p 8083:8081 \
  -e DATABASE_URL=jdbc:postgresql://prod-db:5432/mohe_db \
  -e EXTERNAL_API_BASE_URL=https://api.production.com \
  -e SPRING_PROFILES_ACTIVE=prod,batch \
  -e BATCH_SCHEDULING_ENABLED=true \
  mohebatch:latest
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mohebatch
spec:
  replicas: 2
  selector:
    matchLabels:
      app: mohebatch
  template:
    metadata:
      labels:
        app: mohebatch
    spec:
      containers:
      - name: app
        image: mohebatch:latest
        ports:
        - containerPort: 8080
        - containerPort: 8081
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-secret
              key: url
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health/liveness
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
```

### Environment-Specific Configurations

#### Local Development
```yaml
spring:
  profiles:
    active: local
logging:
  level:
    com.example.ingestion: DEBUG
```

#### Development Environment
```yaml
spring:
  profiles:
    active: dev
app:
  external:
    api:
      base-url: https://api-dev.example.com
```

#### Production Environment
```yaml
spring:
  profiles:
    active: prod
logging:
  level:
    com.example.ingestion: WARN
app:
  batch:
    chunk-size: 1000  # Higher throughput
```

## 🔒 Security Considerations

### Database Security
- Use connection pooling to limit database connections
- Store database credentials in secure vaults (Kubernetes secrets, AWS Secrets Manager)
- Enable SSL/TLS for database connections in production

### API Security
- Implement authentication for external API calls if required
- Use HTTPS for all external communications
- Validate and sanitize all incoming data

### Application Security
- Run application as non-root user in containers
- Use minimal base images (OpenJDK slim)
- Regularly update dependencies for security patches

## 🐛 Troubleshooting

### Common Issues

#### Job Fails to Start
```bash
# Check database connectivity
curl http://localhost:8081/actuator/health

# Check logs for database migration issues
docker-compose logs app | grep flyway
```

#### External API Timeouts
```bash
# Test API connectivity
curl -v http://localhost:3000/health

# Check API response times
curl -w "@curl-format.txt" -o /dev/null http://localhost:3000/api/data
```

#### Memory Issues
```bash
# Monitor JVM memory usage
curl http://localhost:8081/actuator/metrics/jvm.memory.used

# Adjust JVM settings
export JAVA_OPTS="-Xmx2048m -XX:+UseG1GC"
```

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG

# Run with Spring Boot DevTools
./gradlew bootRun --args="--spring.profiles.active=local,debug"
```

### Performance Tuning

#### Batch Performance
- Increase chunk size for higher throughput: `BATCH_CHUNK_SIZE=1000`
- Adjust page size to match API capabilities: `EXTERNAL_API_PAGE_SIZE=500`
- Tune database connection pool: `DB_MAX_POOL_SIZE=50`

#### Database Performance
- Add indexes on frequently queried columns
- Monitor query performance with `show-sql: true`
- Use connection pooling optimizations

## 📚 Additional Resources

- [Spring Batch Documentation](https://spring.io/projects/spring-batch)
- [Spring Boot Actuator Guide](https://spring.io/guides/gs/actuator-service/)
- [PostgreSQL Performance Tuning](https://wiki.postgresql.org/wiki/Performance_Optimization)
- [Micrometer Metrics](https://micrometer.io/docs)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Write tests for new functionality
4. Ensure all tests pass: `./gradlew test`
5. Submit a pull request with detailed description

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
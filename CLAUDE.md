# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## MoheBatch - Batch Processing Server

MoheBatch is a standalone Spring Batch server for processing place data from MoheSpring's database. It handles:
- Crawling place data via MoheCrawler service
- Generating AI descriptions via OpenAI API
- Downloading and managing place images
- Distributed processing with multiple workers (crawling)
- Keyword embedding generation (sequential processing)

## Build and Development Commands

### Gradle Tasks
```bash
# Build the application
./gradlew build

# Run tests
./gradlew test

# Clean build
./gradlew clean build

# Run the application locally
./gradlew bootRun
```

### Docker Development

**메인 서버에서 실행 (모든 서비스 로컬):**
```bash
# Start batch server with crawler and Redis
docker compose up --build

# Start in background
docker compose up -d

# View logs
docker compose logs -f batch

# Stop all services
docker compose down
```

**외부 서버에서 실행 (메인 서버의 Redis/DB/Embedding 사용):**
```bash
# 1. 환경 설정 파일 준비
cp .env.external.example .env
# .env 파일에서 EMBEDDING_SERVICE_URL 등 설정

# 2. 외부 서버용 docker-compose로 실행
docker compose -f docker-compose.external.yml up --build

# 3. 로그 확인
docker compose -f docker-compose.external.yml logs -f batch
```

### Application URLs
- **Health Check**: http://localhost:8081/health
- **Batch Status**: http://localhost:8081/api/batch/status

## API Endpoints

### Crawling Batch API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/batch/start/{workerId}` | Start batch for specific worker (0-2) |
| POST | `/api/batch/start-all` | Start all workers |
| POST | `/api/batch/stop/{workerId}` | Stop specific worker |
| POST | `/api/batch/stop-all` | Stop all workers |
| GET | `/api/batch/status` | Get all workers status |
| GET | `/api/batch/status/{workerId}` | Get specific worker status |
| GET | `/health` | Health check |

### Embedding Batch API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/batch/embedding/start` | Start embedding job (sequential) |
| POST | `/api/batch/embedding/stop` | Stop embedding job |
| GET | `/api/batch/embedding/status` | Get embedding job status |
| GET | `/api/batch/embedding/health` | Check embedding service health |

### Example Usage
```bash
# Start worker 0
curl -X POST http://localhost:8081/api/batch/start/0

# Start all 3 workers
curl -X POST http://localhost:8081/api/batch/start-all

# Check status
curl http://localhost:8081/api/batch/status

# Stop worker 1
curl -X POST http://localhost:8081/api/batch/stop/1

# Start embedding job
curl -X POST http://localhost:8081/api/batch/embedding/start

# Check embedding status
curl http://localhost:8081/api/batch/embedding/status

# Stop embedding job
curl -X POST http://localhost:8081/api/batch/embedding/stop
```

## Architecture Overview

### Core Components

**CrawlingReader**:
- Reads places with `crawler_found = false`
- Distributes data using `ID % totalWorkers = workerId`
- Uses `ORDER BY id ASC` for consistent ordering
- Paging with configurable page size

**CrawlingProcessor** (Async):
- Calls MoheCrawler to fetch place data
- Generates AI descriptions via OpenAI
- Processes in parallel using AsyncItemProcessor

**CrawlingWriter**:
- Clears existing collections (orphanRemoval)
- Saves new data from crawler
- Downloads images (deletes existing before re-downloading)
- Sets `crawler_found = true` on completion
- Auto-updates `updated_at` via @PreUpdate

### Worker Distribution

Data is distributed across 3 workers using modulo:
- Worker 0: Places where `id % 3 = 0`
- Worker 1: Places where `id % 3 = 1`
- Worker 2: Places where `id % 3 = 2`

### Package Structure

```
com.mohe.batch/
├── MoheBatchApplication.java    # Main entry point
├── config/                      # Spring configurations
│   ├── BatchConfig.java        # Async JobLauncher config
│   └── PGvectorType.java       # PGvector Hibernate type
├── controller/                  # REST controllers
│   ├── BatchController.java     # Crawling batch API
│   ├── EmbeddingController.java # Embedding batch API
│   └── HealthController.java    # Health check
├── dto/                         # Data transfer objects
│   ├── crawling/               # Crawler DTOs
│   ├── embedding/              # Embedding DTOs
│   └── ApiResponse.java        # Standard API response
├── entity/                      # JPA entities
│   └── PlaceKeywordEmbedding.java  # Embedding entity
├── job/                         # Spring Batch components
│   ├── CrawlingJobConfig.java  # Crawling job config
│   ├── CrawlingReader.java     # Crawling item reader
│   ├── EmbeddingJobConfig.java # Embedding job config (sequential)
│   └── EmbeddingReader.java    # Embedding item reader
├── repository/                  # Spring Data JPA repositories
│   └── PlaceKeywordEmbeddingRepository.java
└── service/                     # Business services
    ├── BatchStatusService.java  # Worker status tracking
    ├── CrawlingService.java     # Crawler API client
    ├── EmbeddingClient.java     # Embedding service client
    ├── ImageService.java        # Image download
    └── OpenAiDescriptionService.java  # AI description
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| DB_HOST | mohe-postgres | PostgreSQL host |
| DB_PORT | 5432 | PostgreSQL port |
| DB_NAME | mohe_db | Database name |
| DB_USERNAME | mohe_user | Database user |
| DB_PASSWORD | - | Database password |
| CRAWLER_BASE_URL | http://mohe-crawler:4000 | Crawler service URL |
| CRAWLER_TIMEOUT_MINUTES | 30 | Crawler request timeout |
| OPENAI_API_KEY | - | OpenAI API key |
| BATCH_TOTAL_WORKERS | 3 | Total number of workers |
| BATCH_THREADS_PER_WORKER | 5 | Threads per worker |
| BATCH_CHUNK_SIZE | 10 | Items per chunk |
| IMAGE_STORAGE_PATH | /app/images | Image storage directory |
| EMBEDDING_SERVICE_URL | http://localhost:8000 | Embedding service URL |
| BATCH_EMBEDDING_CHUNK_SIZE | 5 | Embedding chunk size |

### Spring Profiles

- `docker`: Containerized deployment (default)
- `local`: Local development with localhost database

## Important Notes

### Database Sharing
MoheBatch shares the same PostgreSQL database with MoheSpring. Ensure proper network configuration when running both services.

### Image Storage
Images are stored in `/app/images/places/{placeId}/` directory. The volume is mounted for persistence.

### Async Processing
Uses Spring Batch's AsyncItemProcessor for parallel processing within each worker. Configure `BATCH_THREADS_PER_WORKER` based on available resources.

### Error Handling
- Failed places are logged but don't stop the batch
- Crawler errors return error response, processing continues
- OpenAI failures skip description generation

### Graceful Shutdown
Use `/api/batch/stop-all` endpoint for graceful shutdown. Workers complete current chunk before stopping.

## External Server Deployment

### Architecture
```
┌─────────────────────────────────────────────────────────────┐
│  외부 서버 (Batch 실행)                                      │
│  ┌─────────────┐  ┌─────────────┐                           │
│  │  MoheBatch  │  │ MoheCrawler │                           │
│  │   :8081     │  │   :2000     │                           │
│  └──────┬──────┘  └─────────────┘                           │
└─────────│───────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│  메인 서버 (100.99.236.50)                                   │
│  ┌──────────┐  ┌──────────┐  ┌───────────────┐              │
│  │ Postgres │  │  Redis   │  │ ImageProcessor│              │
│  │  :16239  │  │  :6380   │  │    :5200      │              │
│  └──────────┘  └──────────┘  └───────────────┘              │
└─────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│  GPU 서버 (임베딩 서비스)                                     │
│  ┌───────────────────────┐                                   │
│  │  Embedding Service    │                                   │
│  │  (Kanana 2.1B)        │                                   │
│  │       :6000           │                                   │
│  └───────────────────────┘                                   │
└─────────────────────────────────────────────────────────────┘
```

### Configuration Files

| 파일 | 용도 |
|------|------|
| `docker-compose.yml` | 메인 서버용 (Redis 포함) |
| `docker-compose.external.yml` | 외부 서버용 (Redis 없음, 메인 서버 연결) |
| `.env.example` | 기본 환경 설정 템플릿 |
| `.env.external.example` | 외부 서버용 환경 설정 템플릿 |

### Required Environment Variables (External Server)

| Variable | Example | Description |
|----------|---------|-------------|
| `MAIN_SERVER_IP` | 100.99.236.50 | 메인 서버 IP (DB, Redis) |
| `OPENAI_API_KEY` | sk-xxx | OpenAI API 키 |
| `EMBEDDING_SERVICE_URL` | http://gpu-server:6000 | 임베딩 서비스 URL |

## Embedding Batch Details

### Processing Flow
```
1. EmbeddingReader: crawl_status=COMPLETED AND embed_status=PENDING 조회
2. EmbeddingProcessor:
   - Place에서 키워드 추출 (최대 9개)
   - EmbeddingClient로 벡터 생성
   - PlaceKeywordEmbedding 엔티티 저장
   - embed_status → COMPLETED 변경
3. EmbeddingWriter: DB 저장
```

### Embedding Service Requirements
- 모델: Kanana Nano 2.1B (kakaocorp/kanana-nano-2.1b-embedding)
- 벡터 차원: 1792
- API: `/embed` (POST, OpenAI 호환)
- 헬스체크: `/health` (GET)

### Monitoring Embedding Progress
```bash
# 임베딩 상태 확인
curl http://localhost:8081/api/batch/embedding/status

# 임베딩 서비스 헬스 체크
curl http://localhost:8081/api/batch/embedding/health

# DB에서 직접 확인
docker exec mohe-postgres psql -U mohe_user -d mohe_db -c \
  "SELECT embed_status, COUNT(*) FROM places GROUP BY embed_status;"
```

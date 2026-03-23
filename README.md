# MoheBatch

> Spring Batch 기반 장소 데이터 크롤링/보강/임베딩 서버

## 기술 스택

- **Framework**: Spring Boot 3.2.0, Java 21, Spring Batch 5.x
- **Database**: PostgreSQL + pgvector (MoheSpring과 공유)
- **Queue**: Redis (작업 큐, 워커 관리, 통계)
- **Processing**: AsyncItemProcessor (5~20 스레드 병렬)

## 시작하기

```bash
# Docker로 실행 (Redis + Batch + Crawler 포함)
docker compose up --build

# 외부 서버 실행 (메인 서버의 Redis/DB 사용)
docker compose -f docker-compose.external.yml up --build

# 로컬 실행
./gradlew bootRun
```

### 환경 변수 (.env)

```bash
DB_HOST=mohe-postgres
DB_PORT=16239
DB_PASSWORD=your_password
OPENAI_API_KEY=your_key
CRAWLER_BASE_URL=http://crawler:2000
EMBEDDING_SERVICE_URL=http://localhost:6000
BATCH_TOTAL_WORKERS=3
BATCH_CHUNK_SIZE=10
```

## API 엔드포인트

### 헬스체크

| Method | Path | 설명 |
|--------|------|------|
| GET | `/health` | 서버 상태 (DB, 워커) |
| GET | `/` | 서비스 정보 및 엔드포인트 목록 |
| GET | `/batch/current-jobs` | 실행 중인 전체 작업 |

### 크롤링 배치 (`/batch`)

| Method | Path | 설명 |
|--------|------|------|
| POST | `/batch/start/{workerId}` | 특정 워커 크롤링 시작 (0~2) |
| POST | `/batch/start-all` | 전체 워커 시작 |
| POST | `/batch/stop/{workerId}` | 특정 워커 중지 |
| POST | `/batch/stop-all` | 전체 워커 중지 |
| GET | `/batch/status` | 전체 워커 상태 |
| GET | `/batch/status/{workerId}` | 특정 워커 상태 |
| GET | `/batch/config` | 서버 설정 (워커수, 스레드, 청크) |

### 업데이트 (`/batch/update`)

| Method | Path | 설명 |
|--------|------|------|
| POST | `/batch/update/start/{workerId}` | 특정 워커 업데이트 |
| POST | `/batch/update/start-all` | 전체 업데이트 (메뉴+이미지+리뷰) |
| POST | `/batch/update/with-description` | 업데이트 + AI 설명 재생성 |
| POST | `/batch/update/menus` | 메뉴만 업데이트 |
| POST | `/batch/update/images` | 이미지만 업데이트 |
| POST | `/batch/update/reviews` | 리뷰만 업데이트 |
| POST | `/batch/update/menus-with-images` | 메뉴 + 메뉴 이미지 업데이트 |
| GET | `/batch/update/status` | 업데이트 배치 상태 |

### 임베딩 (`/batch/embedding`)

| Method | Path | 설명 |
|--------|------|------|
| POST | `/batch/embedding/start` | 임베딩 시작 (레거시, 키워드) |
| POST | `/batch/embedding/stop` | 임베딩 중지 (레거시) |
| GET | `/batch/embedding/status` | 임베딩 상태 (레거시) |
| GET | `/batch/embedding/health` | 임베딩 서비스 헬스체크 |
| POST | `/batch/embedding/keyword/start` | 키워드 임베딩 배치 시작 |
| POST | `/batch/embedding/keyword/stop` | 키워드 임베딩 중지 |
| GET | `/batch/embedding/keyword/status` | 키워드 임베딩 상태 |
| POST | `/batch/embedding/menu/start` | 메뉴 임베딩 시작 |
| POST | `/batch/embedding/menu/stop` | 메뉴 임베딩 중지 |
| GET | `/batch/embedding/menu/status` | 메뉴 임베딩 상태 |
| POST | `/batch/embedding/all/start` | 전체 임베딩 (키워드+메뉴) |
| POST | `/batch/embedding/all/stop` | 전체 임베딩 중지 |
| GET | `/batch/embedding/all/status` | 전체 임베딩 상태 |

### Redis 큐 (`/batch/queue`)

| Method | Path | 설명 |
|--------|------|------|
| POST | `/batch/queue/push/{placeId}` | 단일 장소 큐 등록 |
| POST | `/batch/queue/push-all` | 미처리 전체 장소 큐 등록 |
| POST | `/batch/queue/push-batch` | 장소 ID 목록 큐 등록 |
| GET | `/batch/queue/stats` | 큐 통계 및 워커 상태 |
| GET | `/batch/queue/task/{taskId}` | 작업 진행 상태 |
| GET | `/batch/queue/workers` | 활성 워커 목록 |
| GET | `/batch/queue/failed` | 실패한 장소 ID 목록 |
| POST | `/batch/queue/worker/start` | 큐 워커 시작 |
| POST | `/batch/queue/worker/stop` | 큐 워커 중지 |
| GET | `/batch/queue/worker/status` | 큐 워커 상태 |
| POST | `/batch/queue/retry-failed` | 실패 작업 재시도 |
| DELETE | `/batch/queue/clear` | 전체 큐 초기화 |
| DELETE | `/batch/queue/clear-completed` | 완료 목록 초기화 |
| DELETE | `/batch/queue/clear-failed` | 실패 목록 초기화 |

### AI 설명 생성 (`/batch/description`)

| Method | Path | 설명 |
|--------|------|------|
| POST | `/batch/description/start` | 설명 생성 배치 시작 |
| GET | `/batch/description/count` | 설명 필요 장소 수 |
| GET | `/batch/description/status` | 설명 배치 상태 |

## 아키텍처

### 워커 분배

```
Place ID % totalWorkers = workerId
Worker 0: ID % 3 = 0
Worker 1: ID % 3 = 1
Worker 2: ID % 3 = 2
```

### 처리 파이프라인

```
CrawlingReader (ID 페이지네이션)
  → AsyncItemProcessor (5~20 스레드)
    → CrawlingService → MoheCrawler (Naver Selenium)
    → OpenAI 설명 생성
    → ImageProcessor 이미지 다운로드
  → CrawlingWriter (Fresh Entity Pattern)
    → PostgreSQL
```

### Redis 큐 구조

```
update:priority  → 높은 우선순위 작업
update:pending   → 일반 작업
update:processing → 처리 중 (Set)
update:completed  → 완료 (Set)
update:failed     → 실패 (Set)
workers:registry  → 워커 정보 (Hash)
update:stats      → 전역 통계 (Hash)
```

## 작성자

**Andrew Lim (임석현)** - sjsh1623@gmail.com

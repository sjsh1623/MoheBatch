# MoheBatch API Guide

MoheBatch는 장소 데이터 크롤링을 위한 독립 배치 서버입니다.

## 서버 정보

| 항목 | 값 |
|------|-----|
| 포트 | 8081 |
| 프레임워크 | Spring Boot 3.2.0 + Spring Batch |
| 언어 | Java 21 |

---

## API 엔드포인트

### 1. Health Check

```bash
GET /health
```

**응답 예시:**
```json
{
  "success": true,
  "data": {
    "status": "UP",
    "database": "UP",
    "service": "mohe-batch",
    "workers": {
      "total": 3,
      "running": 0
    },
    "startupTime": "2025-12-21T17:38:37.988468",
    "timestamp": "2025-12-21T17:40:00.000000"
  }
}
```

---

### 2. 배치 상태 조회

#### 전체 상태
```bash
GET /api/batch/status
```

**응답 예시:**
```json
{
  "success": true,
  "data": {
    "totalWorkers": 3,
    "runningCount": 3,
    "workers": [
      {
        "workerId": 0,
        "status": "STARTED",
        "jobExecutionId": 131,
        "processedCount": 150,
        "totalCount": 11591,
        "progressPercent": "1.29",
        "startTime": "2025-12-21T17:51:09",
        "lastUpdated": "2025-12-21T17:55:30"
      },
      {
        "workerId": 1,
        "status": "STARTED",
        "jobExecutionId": 132,
        "processedCount": 145,
        "totalCount": 11599,
        "progressPercent": "1.25"
      },
      {
        "workerId": 2,
        "status": "STARTED",
        "jobExecutionId": 133,
        "processedCount": 148,
        "totalCount": 11640,
        "progressPercent": "1.27"
      }
    ]
  }
}
```

#### 특정 워커 상태
```bash
GET /api/batch/status/{workerId}
```

**예시:**
```bash
curl http://localhost:8081/api/batch/status/0
```

---

### 3. 배치 시작

#### 전체 워커 시작
```bash
POST /api/batch/start-all
```

**응답 예시:**
```json
{
  "success": true,
  "data": {
    "totalWorkers": 3,
    "started": [
      {"workerId": 0, "jobExecutionId": 131, "status": "STARTING"},
      {"workerId": 1, "jobExecutionId": 132, "status": "STARTING"},
      {"workerId": 2, "jobExecutionId": 133, "status": "STARTING"}
    ],
    "failed": []
  }
}
```

#### 특정 워커 시작
```bash
POST /api/batch/start/{workerId}
```

**예시:**
```bash
curl -X POST http://localhost:8081/api/batch/start/0
```

**응답 예시:**
```json
{
  "success": true,
  "data": {
    "workerId": 0,
    "jobExecutionId": 131,
    "status": "STARTING",
    "startTime": "2025-12-21T17:51:09"
  }
}
```

---

### 4. 배치 중지

#### 전체 워커 중지
```bash
POST /api/batch/stop-all
```

**응답 예시:**
```json
{
  "success": true,
  "data": {
    "stopped": [
      {"jobExecutionId": 131, "status": "STOPPING"},
      {"jobExecutionId": 132, "status": "STOPPING"},
      {"jobExecutionId": 133, "status": "STOPPING"}
    ],
    "failed": []
  }
}
```

#### 특정 워커 중지
```bash
POST /api/batch/stop/{workerId}
```

---

## 크롤링 조건

### 데이터 조회 조건

```sql
SELECT id FROM places
WHERE crawler_found = false
AND MOD(id, 3) = :workerId
ORDER BY id ASC
```

| 조건 | 설명 |
|------|------|
| `crawler_found = false` | 아직 크롤링되지 않은 데이터만 처리 |
| `MOD(id, 3) = workerId` | 워커별 데이터 분산 (0, 1, 2) |
| `ORDER BY id ASC` | ID 순서대로 처리 |

### 워커 분산

```
Worker 0: ID % 3 = 0 → ID 3, 6, 9, 12, 15, ...
Worker 1: ID % 3 = 1 → ID 1, 4, 7, 10, 13, ...
Worker 2: ID % 3 = 2 → ID 2, 5, 8, 11, 14, ...
```

### 데이터 처리 후 상태

| 컬럼 | 크롤링 전 | 크롤링 후 |
|------|----------|----------|
| `crawler_found` | `false` | `true` |
| `ready` | `null/false` | `false` |
| `updated_at` | 이전 값 | 현재 시간 (자동) |

> **참고**: `ready = true`는 임베딩 완료 후 MoheSpring에서 설정됩니다.

---

## 처리 파이프라인

### 1. Reader (CrawlingReader)

- `crawler_found = false` 데이터 조회
- 워커별 MOD 분산
- ORDER BY id ASC
- 페이지 단위 로드 (기본 10개)

### 2. Processor (CrawlingJobConfig)

```
1. MoheCrawler 호출 (크롤링)
   ↓
2. OpenAI API 호출 (설명 생성)
   ↓
3. 이미지 다운로드 (기존 삭제 후 재다운로드)
   ↓
4. 데이터 변환
   - descriptions, images, businessHours, sns, reviews 생성
   - keywords 설정 (9개)
   - crawler_found = true 설정
```

### 3. Writer (CrawlingJobConfig)

```
1. Fresh entity 조회
   ↓
2. 기존 컬렉션 삭제 (orphanRemoval)
   ↓
3. 새 데이터 복사
   ↓
4. saveAndFlush (updated_at 자동 갱신)
```

---

## 이미지 처리

### 기존 이미지 삭제 후 재다운로드

```java
// ImageService.java
Path placeImageDir = Paths.get(storagePath, "places", String.valueOf(placeId));

// 1. 기존 디렉토리 삭제
if (Files.exists(placeImageDir)) {
    deleteDirectory(placeImageDir);
}

// 2. 새 디렉토리 생성
Files.createDirectories(placeImageDir);

// 3. 새 이미지 다운로드 (최대 5개)
for (int i = 0; i < Math.min(imageUrls.size(), 5); i++) {
    downloadImage(imageUrl, filePath);
}
```

### 저장 경로

```
/app/images/places/{placeId}/{placeId}_{placeName}_{index}.jpg
```

---

## updated_at 자동 갱신

### @PreUpdate 어노테이션

```java
// Place.java
@PreUpdate
protected void onUpdate() {
    updatedAt = LocalDateTime.now();
}
```

- JPA의 `@PreUpdate` 어노테이션으로 자동 갱신
- `saveAndFlush()` 호출 시 자동으로 현재 시간 설정

---

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `SERVER_PORT` | 8081 | 서버 포트 |
| `DB_HOST` | localhost | PostgreSQL 호스트 |
| `DB_PORT` | 5432 | PostgreSQL 포트 |
| `DB_NAME` | mohe_db | 데이터베이스 이름 |
| `DB_USERNAME` | postgres | DB 사용자 |
| `DB_PASSWORD` | - | DB 비밀번호 |
| `CRAWLER_URL` | http://localhost:4000 | MoheCrawler URL |
| `CRAWLER_TIMEOUT_MINUTES` | 30 | 크롤링 타임아웃 (분) |
| `OPENAI_API_KEY` | - | OpenAI API 키 |
| `BATCH_TOTAL_WORKERS` | 3 | 전체 워커 수 |
| `BATCH_THREADS_PER_WORKER` | 5 | 워커당 스레드 수 |
| `BATCH_CHUNK_SIZE` | 10 | 청크 크기 |
| `IMAGE_STORAGE_PATH` | /app/images | 이미지 저장 경로 |
| `EMBEDDING_SERVICE_URL` | http://localhost:8000 | 임베딩 서버 URL |
| `BATCH_EMBEDDING_CHUNK_SIZE` | 5 | 임베딩 청크 크기 |

---

## 실행 예시

### Docker 실행

```bash
# 1. 환경 변수 설정
cp .env.example .env
# .env 파일 편집

# 2. Docker 실행
docker compose up --build -d

# 3. 상태 확인
curl http://localhost:8081/health

# 4. 모든 워커 시작
curl -X POST http://localhost:8081/api/batch/start-all

# 5. 상태 모니터링
watch -n 5 'curl -s http://localhost:8081/api/batch/status'

# 6. 중지
curl -X POST http://localhost:8081/api/batch/stop-all
```

### 로컬 실행

```bash
# 환경 변수와 함께 실행
DB_HOST=localhost \
DB_PORT=16239 \
DB_NAME=mohe_db \
DB_USERNAME=mohe_user \
DB_PASSWORD=mohe_password \
CRAWLER_URL=http://localhost:4000 \
./gradlew bootRun
```

---

## 임베딩 API 엔드포인트

### 1. 임베딩 배치 시작

```bash
POST /api/batch/embedding/start
```

**응답 예시:**
```json
{
  "success": true,
  "data": {
    "jobExecutionId": 150,
    "status": "STARTING",
    "startTime": "2025-12-21T18:00:00",
    "pendingCount": 5000
  }
}
```

### 2. 임베딩 배치 중지

```bash
POST /api/batch/embedding/stop
```

### 3. 임베딩 상태 조회

```bash
GET /api/batch/embedding/status
```

**응답 예시:**
```json
{
  "success": true,
  "data": {
    "pendingCount": 4500,
    "embeddedCount": 500,
    "totalEmbeddings": 4500,
    "jobExecutionId": 150,
    "jobStatus": "STARTED",
    "readCount": 500,
    "writeCount": 500,
    "skipCount": 0,
    "embeddingServiceUrl": "http://localhost:8000",
    "embeddingServiceAvailable": true
  }
}
```

### 4. 임베딩 서비스 헬스 체크

```bash
GET /api/batch/embedding/health
```

**응답 예시:**
```json
{
  "success": true,
  "data": {
    "serviceUrl": "http://localhost:8000",
    "available": true
  }
}
```

---

## 임베딩 조건

### 데이터 조회 조건

```sql
SELECT id FROM places
WHERE crawler_found = true
AND ready = false
ORDER BY id ASC
```

| 조건 | 설명 |
|------|------|
| `crawler_found = true` | 크롤링 완료된 데이터만 |
| `ready = false` | 아직 임베딩 안 됨 |
| `ORDER BY id ASC` | ID 순서대로 처리 |

### 처리 방식

- **순차 처리** (병렬 없음)
- 키워드 최대 9개씩 임베딩
- 1792 차원 벡터 생성
- `place_keyword_embeddings` 테이블 저장

### 데이터 처리 후 상태

| 컬럼 | 임베딩 전 | 임베딩 후 |
|------|----------|----------|
| `crawler_found` | `true` | `true` |
| `ready` | `false` | `true` |

---

## 관련 서비스

| 서비스 | 역할 | 포트 |
|--------|------|------|
| **MoheBatch** | 크롤링 + 임베딩 배치 | 8081 |
| **MoheSpring** | API 서버 | 8080 |
| **MoheCrawler** | 크롤러 (Flask + Selenium) | 4000 |
| **MoheEmbedding** | 임베딩 서버 (Kanana) | 8000 |
| **PostgreSQL** | 데이터베이스 | 5432/16239 |

---

## 데이터 처리 전체 흐름

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. MoheBatch (크롤링)                                            │
│    - crawler_found = false → true                               │
│    - ready = false (유지)                                        │
│    - 이미지 다운로드, 설명 생성, 키워드 추출                       │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. MoheBatch (임베딩)                                            │
│    - crawler_found = true AND ready = false 조회                 │
│    - 키워드 9개 → 벡터 임베딩 (1792 차원)                         │
│    - place_keyword_embeddings 테이블 저장                        │
│    - ready = true 설정                                           │
│    - 순차 처리 (병렬 없음)                                        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. MoheSpring API 서비스                                         │
│    - ready = true 데이터만 추천에 사용                            │
│    - 벡터 유사도 검색으로 장소 추천                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## 에러 처리

### 크롤링 실패 시

```java
place.setCrawlerFound(false);
place.setReady(false);
return place;  // 다음 배치에서 재시도
```

### 이미지 다운로드 실패 시

- 해당 이미지만 스킵
- 나머지 이미지는 계속 다운로드
- 로그에 경고 출력

### OpenAI API 실패 시

- Fallback 설명 사용 (AI 요약 또는 원본 설명)
- 키워드는 카테고리에서 추출
- 프로세스 계속 진행

---

## 로그 확인

### Docker 로그

```bash
docker compose logs -f batch
```

### 주요 로그 패턴

```
Worker 0 initialized - 11591 places to process
Worker 0 - Loaded page 1: 10 IDs (hasNext: true)
Starting crawl for '카페명' (ID: 123)
Successfully crawled '카페명' - Reviews: 50, Images: 5
Saved place '카페명' (ID: 123, crawler_found=true, ready=false)
```

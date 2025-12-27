# 맥북 에어 배치 서버 설정 가이드

## 개요
이 가이드는 맥북 에어에서 배치 워커 2, 3을 실행하기 위한 설정입니다.
메인 맥에서 워커 0, 1이 실행 중이며, 데이터 중복 처리 없이 분산 처리됩니다.

### 데이터 분배 방식
```
Place ID % 4 = 0 → 워커 0 (메인 맥)
Place ID % 4 = 1 → 워커 1 (메인 맥)
Place ID % 4 = 2 → 워커 2 (맥북 에어)
Place ID % 4 = 3 → 워커 3 (맥북 에어)
```

---

## 1. 사전 요구사항

### 필수 설치
- Docker Desktop
- Tailscale (VPN - 메인 맥과 동일 네트워크)

### Tailscale 연결 확인
```bash
# Tailscale 상태 확인
tailscale status

# 메인 맥 연결 테스트 (IP: 100.99.236.50)
ping 100.99.236.50
```

---

## 2. 프로젝트 복사

메인 맥에서 맥북 에어로 다음 폴더들을 복사:

```bash
# 방법 1: Git clone
git clone https://github.com/sjsh1623/MoheBatch.git
git clone https://github.com/sjsh1623/MoheCrawler.git

# 방법 2: AirDrop 또는 외장 드라이브
# MoheBatch 폴더
# MoheCrawler 폴더
```

폴더 구조:
```
~/Desktop/Mohe/
├── MoheBatch/
└── MoheCrawler/
```

---

## 3. 환경 설정

### 3.1 OpenAI API Key 설정 (선택사항)
```bash
cd ~/Desktop/Mohe/MoheBatch
echo "OPENAI_API_KEY=sk-your-api-key" > .env
```

---

## 4. 서비스 시작

### 4.1 Docker 컨테이너 빌드 및 시작
```bash
cd ~/Desktop/Mohe/MoheBatch
docker compose -f docker-compose.macbook-air.yml up -d --build
```

### 4.2 서비스 상태 확인
```bash
# 헬스 체크 (약 30초 대기 후)
curl http://localhost:8081/health

# 컨테이너 상태
docker ps
```

예상 출력:
```
NAMES                    STATUS
mohe-batch-air           Up (healthy)
mohe-batch-crawler-air   Up (healthy)
```

---

## 5. 배치 워커 시작

### 5.1 워커 2, 3 시작
```bash
# 워커 2 시작
curl -X POST http://localhost:8081/batch/start/2

# 워커 3 시작
curl -X POST http://localhost:8081/batch/start/3
```

### 5.2 상태 확인
```bash
curl http://localhost:8081/batch/status | python3 -m json.tool
```

---

## 6. 모니터링

### 로그 확인
```bash
# 배치 서버 로그
docker logs -f mohe-batch-air

# 크롤러 로그
docker logs -f mohe-batch-crawler-air
```

### 주요 확인 사항
1. **크롤링 진행**: `🚀 크롤링 시작`, `✅ 크롤링 완료` 로그
2. **이미지 저장**: `🖼️ 이미지 저장` 로그
3. **에러**: `❌`, `ERROR` 로그

---

## 7. 서비스 중지

```bash
# 워커 중지
curl -X POST http://localhost:8081/batch/stop/2
curl -X POST http://localhost:8081/batch/stop/3

# 컨테이너 중지
docker compose -f docker-compose.macbook-air.yml down
```

---

## 8. 네트워크 구성

### 연결 대상 (메인 맥 서비스)
| 서비스 | 주소 | 용도 |
|--------|------|------|
| PostgreSQL | 100.99.236.50:16239 | 데이터베이스 |
| ImageProcessor | 100.99.236.50:5200 | 이미지 저장 |
| Embedding | 100.99.236.50:6000 | 키워드 임베딩 |

### 로컬 서비스 (맥북 에어)
| 서비스 | 포트 | 용도 |
|--------|------|------|
| Batch Server | 8081 | 배치 API |
| Crawler | 2000 | 웹 크롤링 |

---

## 9. 트러블슈팅

### 문제: 메인 맥 서비스 연결 실패
```bash
# Tailscale 연결 확인
tailscale status

# 포트 연결 테스트
nc -zv 100.99.236.50 16239  # DB
nc -zv 100.99.236.50 5200   # ImageProcessor
```

### 문제: OOM (메모리 부족)
```bash
# 메모리 사용량 확인
docker stats

# 컨테이너 재시작
docker compose -f docker-compose.macbook-air.yml restart
```

### 문제: 이미지 저장 실패
이미지는 메인 맥의 ImageProcessor를 통해 저장됩니다.
메인 맥의 `moheimageprocessor-app-1` 컨테이너가 실행 중인지 확인하세요.

---

## 10. 주의사항

1. **워커 ID 고정**: 맥북 에어에서는 반드시 워커 2, 3만 실행
2. **totalWorkers 일치**: 양쪽 모두 `BATCH_TOTAL_WORKERS=4`로 동일해야 함
3. **이미지 저장소**: 이미지는 메인 맥에 저장됨 (맥북 에어 로컬에 저장 안됨)
4. **DB 공유**: 동일한 PostgreSQL 사용, 데이터 중복 없음

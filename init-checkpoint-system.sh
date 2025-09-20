#!/bin/bash

# 체크포인트 시스템 초기화 스크립트
# 배치 중단/재시작 기능을 위한 데이터베이스 테이블 생성

echo "🗃️ 체크포인트 시스템 초기화 시작..."

# 환경 변수 로드
if [ -f .env ]; then
    echo "📋 환경 변수 로드 중..."
    export $(cat .env | grep -v '^#' | xargs)
fi

# 데이터베이스 연결 정보
DB_HOST=${DATABASE_HOST:-localhost}
DB_PORT=${DATABASE_PORT:-5432}
DB_NAME=${DATABASE_NAME:-mohe_db}
DB_USER=${DATABASE_USERNAME:-mohe_user}
DB_PASSWORD=${DATABASE_PASSWORD:-mohe_password}

echo "🔗 데이터베이스 연결: $DB_HOST:$DB_PORT/$DB_NAME"

# PostgreSQL에 연결하여 체크포인트 테이블 생성
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME << 'EOF'

-- 체크포인트 테이블이 이미 존재하는지 확인
\echo '📋 기존 체크포인트 테이블 확인 중...'

DO $$
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'batch_checkpoint') THEN
        RAISE NOTICE '✅ batch_checkpoint 테이블이 이미 존재합니다';
    ELSE
        RAISE NOTICE '🆕 batch_checkpoint 테이블을 생성합니다';
    END IF;
END
$$;

-- 체크포인트 테이블 생성 (존재하지 않는 경우에만)
CREATE TABLE IF NOT EXISTS batch_checkpoint (
    id BIGSERIAL PRIMARY KEY,
    batch_name VARCHAR(100) NOT NULL,
    region_type VARCHAR(50) NOT NULL,
    region_code VARCHAR(20) NOT NULL,
    region_name VARCHAR(200) NOT NULL,
    parent_code VARCHAR(20),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    processed_count INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(batch_name, region_type, region_code)
);

-- 배치 실행 메타데이터 테이블 생성
CREATE TABLE IF NOT EXISTS batch_execution_metadata (
    id BIGSERIAL PRIMARY KEY,
    batch_name VARCHAR(100) NOT NULL,
    execution_id VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    total_regions INTEGER DEFAULT 0,
    completed_regions INTEGER DEFAULT 0,
    failed_regions INTEGER DEFAULT 0,
    last_checkpoint_id BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (last_checkpoint_id) REFERENCES batch_checkpoint(id)
);

-- 인덱스 생성 (존재하지 않는 경우에만)
CREATE INDEX IF NOT EXISTS idx_batch_checkpoint_status ON batch_checkpoint(batch_name, status);
CREATE INDEX IF NOT EXISTS idx_batch_checkpoint_region ON batch_checkpoint(region_type, region_code);
CREATE INDEX IF NOT EXISTS idx_batch_checkpoint_parent ON batch_checkpoint(parent_code);

-- 트리거 함수 생성 (이미 존재하면 대체)
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 트리거 생성 (이미 존재하면 무시)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_batch_checkpoint_updated_at') THEN
        CREATE TRIGGER update_batch_checkpoint_updated_at
            BEFORE UPDATE ON batch_checkpoint
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_batch_execution_metadata_updated_at') THEN
        CREATE TRIGGER update_batch_execution_metadata_updated_at
            BEFORE UPDATE ON batch_execution_metadata
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END
$$;

-- 현재 체크포인트 상태 확인
\echo '📊 현재 체크포인트 상태:'
SELECT
    batch_name,
    COUNT(*) as total_regions,
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed,
    COUNT(CASE WHEN status = 'PROCESSING' THEN 1 END) as processing,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) as pending
FROM batch_checkpoint
GROUP BY batch_name;

\echo '✅ 체크포인트 시스템 초기화 완료!'

EOF

if [ $? -eq 0 ]; then
    echo "🎉 체크포인트 시스템이 성공적으로 초기화되었습니다!"
    echo "🔄 이제 배치가 중단되면 마지막 처리된 지역부터 자동으로 재시작됩니다."
else
    echo "❌ 체크포인트 시스템 초기화 실패"
    exit 1
fi
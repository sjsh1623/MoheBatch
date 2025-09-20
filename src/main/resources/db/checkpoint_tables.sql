-- 배치 체크포인트 시스템을 위한 테이블 생성
-- Government API 기반 진행 상태 추적

-- 정부 API 진행 상태 테이블
CREATE TABLE IF NOT EXISTS batch_checkpoint (
    id BIGSERIAL PRIMARY KEY,
    batch_name VARCHAR(100) NOT NULL,
    region_type VARCHAR(50) NOT NULL,  -- 'sido', 'sigungu', 'dong' 등
    region_code VARCHAR(20) NOT NULL,  -- 행정구역 코드
    region_name VARCHAR(200) NOT NULL, -- 행정구역 이름
    parent_code VARCHAR(20),           -- 상위 행정구역 코드
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING, PROCESSING, COMPLETED, FAILED
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    processed_count INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(batch_name, region_type, region_code)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_batch_checkpoint_status ON batch_checkpoint(batch_name, status);
CREATE INDEX IF NOT EXISTS idx_batch_checkpoint_region ON batch_checkpoint(region_type, region_code);
CREATE INDEX IF NOT EXISTS idx_batch_checkpoint_parent ON batch_checkpoint(parent_code);

-- 배치 실행 메타데이터 테이블
CREATE TABLE IF NOT EXISTS batch_execution_metadata (
    id BIGSERIAL PRIMARY KEY,
    batch_name VARCHAR(100) NOT NULL,
    execution_id VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING', -- RUNNING, COMPLETED, FAILED, INTERRUPTED
    total_regions INTEGER DEFAULT 0,
    completed_regions INTEGER DEFAULT 0,
    failed_regions INTEGER DEFAULT 0,
    last_checkpoint_id BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (last_checkpoint_id) REFERENCES batch_checkpoint(id)
);

-- 트리거 생성 - updated_at 자동 업데이트
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_batch_checkpoint_updated_at
    BEFORE UPDATE ON batch_checkpoint
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_batch_execution_metadata_updated_at
    BEFORE UPDATE ON batch_execution_metadata
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
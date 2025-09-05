-- 데이터베이스 재초기화 메커니즘
-- 이 스크립트는 개발 및 테스트 환경에서 데이터베이스를 안전하게 리셋할 수 있는 기능을 제공합니다.

-- Step 1: 마이그레이션 로그 테이블 생성 (없는 경우)
CREATE TABLE IF NOT EXISTS migration_log (
    id BIGSERIAL PRIMARY KEY,
    migration_version VARCHAR(50) NOT NULL,
    description TEXT,
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    execution_time_ms BIGINT,
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT,
    UNIQUE(migration_version)
);

-- Step 2: 시스템 설정 테이블 생성
CREATE TABLE IF NOT EXISTS system_settings (
    setting_key VARCHAR(100) PRIMARY KEY,
    setting_value TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Step 3: 데이터베이스 리셋 관련 함수들
-- 환경 체크 함수 (프로덕션 환경에서는 리셋 방지)
CREATE OR REPLACE FUNCTION is_safe_for_reset()
RETURNS BOOLEAN AS $$
DECLARE
    current_env TEXT;
BEGIN
    -- 환경변수 또는 설정에서 현재 환경 확인
    SELECT setting_value INTO current_env 
    FROM system_settings 
    WHERE setting_key = 'environment';
    
    -- 기본값이 없으면 'development'로 간주
    IF current_env IS NULL THEN
        current_env := 'development';
    END IF;
    
    -- production 환경이 아닌 경우에만 true 반환
    RETURN current_env != 'production';
END;
$$ LANGUAGE plpgsql;

-- 테이블 데이터 백업 함수
CREATE OR REPLACE FUNCTION backup_table_data(table_name TEXT)
RETURNS VOID AS $$
DECLARE
    backup_table_name TEXT;
    record_count INTEGER;
BEGIN
    -- 백업 테이블명 생성
    backup_table_name := table_name || '_backup_' || to_char(NOW(), 'YYYYMMDD_HH24MISS');
    
    -- 데이터 존재 여부 확인
    EXECUTE format('SELECT COUNT(*) FROM %I', table_name) INTO record_count;
    
    IF record_count > 0 THEN
        -- 백업 테이블 생성
        EXECUTE format('CREATE TABLE %I AS SELECT * FROM %I', backup_table_name, table_name);
        
        RAISE NOTICE 'Backed up % records from % to %', record_count, table_name, backup_table_name;
        
        -- 백업 정보를 로그에 기록
        INSERT INTO migration_log (migration_version, description, executed_at) 
        VALUES (
            'BACKUP_' || upper(table_name), 
            format('Backup created: %s with %s records', backup_table_name, record_count),
            NOW()
        );
    ELSE
        RAISE NOTICE 'Table % is empty, skipping backup', table_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 단일 테이블 리셋 함수
CREATE OR REPLACE FUNCTION reset_table_data(table_name TEXT, create_backup BOOLEAN DEFAULT TRUE)
RETURNS VOID AS $$
BEGIN
    -- 안전성 체크
    IF NOT is_safe_for_reset() THEN
        RAISE EXCEPTION 'Database reset is not allowed in production environment';
    END IF;
    
    -- 백업 생성 (요청된 경우)
    IF create_backup THEN
        PERFORM backup_table_data(table_name);
    END IF;
    
    -- 외래키 제약조건 일시적 비활성화
    SET session_replication_role = 'replica';
    
    -- 테이블 데이터 삭제
    EXECUTE format('DELETE FROM %I', table_name);
    
    -- 시퀀스 리셋 (ID가 있는 테이블의 경우)
    BEGIN
        EXECUTE format('ALTER SEQUENCE %I_id_seq RESTART WITH 1', table_name);
    EXCEPTION WHEN others THEN
        -- 시퀀스가 없는 테이블은 무시
        NULL;
    END;
    
    -- 외래키 제약조건 재활성화
    SET session_replication_role = 'origin';
    
    RAISE NOTICE 'Successfully reset table: %', table_name;
END;
$$ LANGUAGE plpgsql;

-- 전체 장소 데이터 리셋 함수
CREATE OR REPLACE FUNCTION reset_all_place_data(create_backup BOOLEAN DEFAULT TRUE)
RETURNS VOID AS $$
DECLARE
    table_list TEXT[] := ARRAY[
        'place_external_raw',
        'place_mbti_descriptions', 
        'place_similarity',
        'place_similarity_topk',
        'place_similarity_mbti_topk',
        'user_preference_vector',
        'place_description_vector',
        'vector_similarity',
        'place_keyword_extraction',
        'bookmarks',
        'recent_views',
        'prompts',
        'places'
    ];
    table_name TEXT;
BEGIN
    -- 안전성 체크
    IF NOT is_safe_for_reset() THEN
        RAISE EXCEPTION 'Database reset is not allowed in production environment';
    END IF;
    
    RAISE NOTICE 'Starting complete place data reset...';
    
    -- 각 테이블을 의존성 순서대로 리셋
    FOREACH table_name IN ARRAY table_list LOOP
        BEGIN
            PERFORM reset_table_data(table_name, create_backup);
        EXCEPTION WHEN others THEN
            RAISE WARNING 'Failed to reset table %: %', table_name, SQLERRM;
        END;
    END LOOP;
    
    -- 리셋 완료 기록
    INSERT INTO migration_log (migration_version, description, executed_at) 
    VALUES ('RESET_ALL_PLACES', 'Complete place data reset executed', NOW());
    
    RAISE NOTICE 'Complete place data reset finished';
END;
$$ LANGUAGE plpgsql;

-- 사용자 데이터는 보존하고 장소 데이터만 리셋하는 함수
CREATE OR REPLACE FUNCTION reset_places_only(create_backup BOOLEAN DEFAULT TRUE)
RETURNS VOID AS $$
BEGIN
    -- 안전성 체크
    IF NOT is_safe_for_reset() THEN
        RAISE EXCEPTION 'Database reset is not allowed in production environment';
    END IF;
    
    RAISE NOTICE 'Starting places-only reset (preserving user data)...';
    
    -- 사용자 북마크와 연관된 데이터 먼저 정리
    PERFORM reset_table_data('bookmarks', create_backup);
    PERFORM reset_table_data('recent_views', create_backup);
    PERFORM reset_table_data('prompts', create_backup);
    
    -- 장소 관련 데이터 정리
    PERFORM reset_table_data('place_external_raw', create_backup);
    PERFORM reset_table_data('place_mbti_descriptions', create_backup);
    PERFORM reset_table_data('place_similarity', create_backup);
    PERFORM reset_table_data('place_similarity_topk', create_backup);
    PERFORM reset_table_data('place_keyword_extraction', create_backup);
    PERFORM reset_table_data('places', create_backup);
    
    -- 리셋 완료 기록
    INSERT INTO migration_log (migration_version, description, executed_at) 
    VALUES ('RESET_PLACES_ONLY', 'Places data reset executed (users preserved)', NOW());
    
    RAISE NOTICE 'Places-only reset finished';
END;
$$ LANGUAGE plpgsql;

-- 백업 테이블 정리 함수 (오래된 백업 테이블들 삭제)
CREATE OR REPLACE FUNCTION cleanup_old_backups(days_to_keep INTEGER DEFAULT 7)
RETURNS VOID AS $$
DECLARE
    backup_table RECORD;
    table_creation_date TIMESTAMP;
    cutoff_date TIMESTAMP;
BEGIN
    cutoff_date := NOW() - INTERVAL '1 day' * days_to_keep;
    
    -- _backup_ 패턴을 가진 테이블들 찾기
    FOR backup_table IN 
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
          AND tablename LIKE '%_backup_%'
    LOOP
        BEGIN
            -- 테이블 생성 시간 추출 (테이블명에서)
            SELECT to_timestamp(
                substring(backup_table.tablename from '.*_backup_(.*)$'), 
                'YYYYMMDD_HH24MISS'
            ) INTO table_creation_date;
            
            -- 오래된 백업 테이블 삭제
            IF table_creation_date < cutoff_date THEN
                EXECUTE format('DROP TABLE IF EXISTS %I.%I', backup_table.schemaname, backup_table.tablename);
                RAISE NOTICE 'Deleted old backup table: %', backup_table.tablename;
            END IF;
            
        EXCEPTION WHEN others THEN
            -- 날짜 파싱 실패 등의 경우 무시
            RAISE WARNING 'Could not process backup table %: %', backup_table.tablename, SQLERRM;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Step 4: 기본 시스템 설정 입력
INSERT INTO system_settings (setting_key, setting_value, description) 
VALUES 
    ('environment', 'development', '현재 환경 설정 (development/staging/production)'),
    ('reset_enabled', 'true', '데이터베이스 리셋 기능 활성화 여부'),
    ('backup_retention_days', '7', '백업 테이블 보관 기간 (일)')
ON CONFLICT (setting_key) DO NOTHING;

-- Step 5: 정기적인 백업 정리를 위한 스케줄링 정보 기록
COMMENT ON FUNCTION cleanup_old_backups IS '오래된 백업 테이블들을 정리합니다. 일주일에 한 번 실행하는 것을 권장합니다.';
COMMENT ON FUNCTION reset_all_place_data IS '모든 장소 관련 데이터를 리셋합니다. 프로덕션 환경에서는 실행되지 않습니다.';
COMMENT ON FUNCTION reset_places_only IS '사용자 데이터는 보존하고 장소 데이터만 리셋합니다.';

-- 마이그레이션 완료 기록
INSERT INTO migration_log (migration_version, description, executed_at) 
VALUES ('V6', 'Database reset mechanism implementation', NOW())
ON CONFLICT DO NOTHING;
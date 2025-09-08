-- V2: Insert admin user with admin/admin credentials

-- Insert admin user with pre-hashed BCrypt password
-- Password: admin
-- BCrypt hash: $2a$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2uheWG/igi.
INSERT INTO users (
    email, 
    password_hash, 
    nickname, 
    is_onboarding_completed,
    created_at, 
    updated_at
) 
VALUES (
    'admin@mohe.com',
    '$2a$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2uheWG/igi.',
    'admin',
    true,
    NOW(),
    NOW()
) ON CONFLICT (email) DO NOTHING;

-- Log the admin user creation
INSERT INTO migration_log (migration_version, description) 
VALUES ('V2', 'Added admin user with email: admin@mohe.com and password: admin');
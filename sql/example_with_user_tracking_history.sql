-- Generated History Tables and Triggers
-- This file contains history tables and triggers for temporal data tracking

-- History table and triggers for: users

CREATE TABLE users_history (
    id SERIAL,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    last_login_at TIMESTAMP,
    login_count INTEGER DEFAULT 0,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D'))
);

CREATE INDEX idx_users_history_valid_from ON users_history (valid_from);
CREATE INDEX idx_users_history_valid_to ON users_history (valid_to);

-- Insert trigger for users
CREATE OR REPLACE FUNCTION users_insert_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO users_history (id, username, email, created_at, updated_at, is_active, last_login_at, login_count, valid_from, operation)
    VALUES (NEW.id, NEW.username, NEW.email, NEW.created_at, NEW.updated_at, NEW.is_active, NEW.last_login_at, NEW.login_count, CURRENT_TIMESTAMP, 'I');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_insert_trigger
    AFTER INSERT ON users
    FOR EACH ROW
    EXECUTE FUNCTION users_insert_history();

-- Update trigger for users
CREATE OR REPLACE FUNCTION users_update_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE users_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND id = OLD.id;
    INSERT INTO users_history (id, username, email, created_at, updated_at, is_active, last_login_at, login_count, valid_from, operation)
    VALUES (NEW.id, NEW.username, NEW.email, NEW.created_at, NEW.updated_at, NEW.is_active, NEW.last_login_at, NEW.login_count, CURRENT_TIMESTAMP, 'U');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_update_trigger
    AFTER UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION users_update_history();

-- Delete trigger for users
CREATE OR REPLACE FUNCTION users_delete_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE users_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND id = OLD.id;
    INSERT INTO users_history (id, username, email, created_at, updated_at, is_active, last_login_at, login_count, valid_from, operation)
    VALUES (OLD.id, OLD.username, OLD.email, OLD.created_at, OLD.updated_at, OLD.is_active, OLD.last_login_at, OLD.login_count, CURRENT_TIMESTAMP, 'D');
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_delete_trigger
    BEFORE DELETE ON users
    FOR EACH ROW
    EXECUTE FUNCTION users_delete_history();


--------------------------------------------------------------------------------

-- History table and triggers for: user_sessions

CREATE TABLE user_sessions_history (
    session_id UUID DEFAULT gen_random_uuid(),
    user_id INTEGER NOT NULL,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D'))
);

CREATE INDEX idx_user_sessions_history_valid_from ON user_sessions_history (valid_from);
CREATE INDEX idx_user_sessions_history_valid_to ON user_sessions_history (valid_to);

-- Insert trigger for user_sessions
CREATE OR REPLACE FUNCTION user_sessions_insert_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO user_sessions_history (session_id, user_id, ip_address, user_agent, created_at, expires_at, is_active, valid_from, operation)
    VALUES (NEW.session_id, NEW.user_id, NEW.ip_address, NEW.user_agent, NEW.created_at, NEW.expires_at, NEW.is_active, CURRENT_TIMESTAMP, 'I');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_sessions_insert_trigger
    AFTER INSERT ON user_sessions
    FOR EACH ROW
    EXECUTE FUNCTION user_sessions_insert_history();

-- Update trigger for user_sessions
CREATE OR REPLACE FUNCTION user_sessions_update_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE user_sessions_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND session_id = OLD.session_id;
    INSERT INTO user_sessions_history (session_id, user_id, ip_address, user_agent, created_at, expires_at, is_active, valid_from, operation)
    VALUES (NEW.session_id, NEW.user_id, NEW.ip_address, NEW.user_agent, NEW.created_at, NEW.expires_at, NEW.is_active, CURRENT_TIMESTAMP, 'U');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_sessions_update_trigger
    AFTER UPDATE ON user_sessions
    FOR EACH ROW
    EXECUTE FUNCTION user_sessions_update_history();

-- Delete trigger for user_sessions
CREATE OR REPLACE FUNCTION user_sessions_delete_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE user_sessions_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND session_id = OLD.session_id;
    INSERT INTO user_sessions_history (session_id, user_id, ip_address, user_agent, created_at, expires_at, is_active, valid_from, operation)
    VALUES (OLD.session_id, OLD.user_id, OLD.ip_address, OLD.user_agent, OLD.created_at, OLD.expires_at, OLD.is_active, CURRENT_TIMESTAMP, 'D');
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_sessions_delete_trigger
    BEFORE DELETE ON user_sessions
    FOR EACH ROW
    EXECUTE FUNCTION user_sessions_delete_history();


--------------------------------------------------------------------------------

-- History table and triggers for: user_activity_log

CREATE TABLE user_activity_log_history (
    id SERIAL,
    user_id INTEGER NOT NULL,
    activity_type VARCHAR(50) NOT NULL,
    description TEXT,
    ip_address INET,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D'))
);

CREATE INDEX idx_user_activity_log_history_valid_from ON user_activity_log_history (valid_from);
CREATE INDEX idx_user_activity_log_history_valid_to ON user_activity_log_history (valid_to);

-- Insert trigger for user_activity_log
CREATE OR REPLACE FUNCTION user_activity_log_insert_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO user_activity_log_history (id, user_id, activity_type, description, ip_address, created_at, metadata, valid_from, operation)
    VALUES (NEW.id, NEW.user_id, NEW.activity_type, NEW.description, NEW.ip_address, NEW.created_at, NEW.metadata, CURRENT_TIMESTAMP, 'I');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_activity_log_insert_trigger
    AFTER INSERT ON user_activity_log
    FOR EACH ROW
    EXECUTE FUNCTION user_activity_log_insert_history();

-- Update trigger for user_activity_log
CREATE OR REPLACE FUNCTION user_activity_log_update_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE user_activity_log_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND id = OLD.id;
    INSERT INTO user_activity_log_history (id, user_id, activity_type, description, ip_address, created_at, metadata, valid_from, operation)
    VALUES (NEW.id, NEW.user_id, NEW.activity_type, NEW.description, NEW.ip_address, NEW.created_at, NEW.metadata, CURRENT_TIMESTAMP, 'U');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_activity_log_update_trigger
    AFTER UPDATE ON user_activity_log
    FOR EACH ROW
    EXECUTE FUNCTION user_activity_log_update_history();

-- Delete trigger for user_activity_log
CREATE OR REPLACE FUNCTION user_activity_log_delete_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE user_activity_log_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND id = OLD.id;
    INSERT INTO user_activity_log_history (id, user_id, activity_type, description, ip_address, created_at, metadata, valid_from, operation)
    VALUES (OLD.id, OLD.user_id, OLD.activity_type, OLD.description, OLD.ip_address, OLD.created_at, OLD.metadata, CURRENT_TIMESTAMP, 'D');
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_activity_log_delete_trigger
    BEFORE DELETE ON user_activity_log
    FOR EACH ROW
    EXECUTE FUNCTION user_activity_log_delete_history();


--------------------------------------------------------------------------------

-- History table and triggers for: user_preferences

CREATE TABLE user_preferences_history (
    user_id INTEGER,
    theme VARCHAR(20) DEFAULT 'light',
    language VARCHAR(10) DEFAULT 'en',
    timezone VARCHAR(50) DEFAULT 'UTC',
    email_notifications BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D'))
);

CREATE INDEX idx_user_preferences_history_valid_from ON user_preferences_history (valid_from);
CREATE INDEX idx_user_preferences_history_valid_to ON user_preferences_history (valid_to);

-- Insert trigger for user_preferences
CREATE OR REPLACE FUNCTION user_preferences_insert_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO user_preferences_history (user_id, theme, language, timezone, email_notifications, updated_at, valid_from, operation)
    VALUES (NEW.user_id, NEW.theme, NEW.language, NEW.timezone, NEW.email_notifications, NEW.updated_at, CURRENT_TIMESTAMP, 'I');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_preferences_insert_trigger
    AFTER INSERT ON user_preferences
    FOR EACH ROW
    EXECUTE FUNCTION user_preferences_insert_history();

-- Update trigger for user_preferences
CREATE OR REPLACE FUNCTION user_preferences_update_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE user_preferences_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND user_id = OLD.user_id;
    INSERT INTO user_preferences_history (user_id, theme, language, timezone, email_notifications, updated_at, valid_from, operation)
    VALUES (NEW.user_id, NEW.theme, NEW.language, NEW.timezone, NEW.email_notifications, NEW.updated_at, CURRENT_TIMESTAMP, 'U');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_preferences_update_trigger
    AFTER UPDATE ON user_preferences
    FOR EACH ROW
    EXECUTE FUNCTION user_preferences_update_history();

-- Delete trigger for user_preferences
CREATE OR REPLACE FUNCTION user_preferences_delete_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE user_preferences_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND user_id = OLD.user_id;
    INSERT INTO user_preferences_history (user_id, theme, language, timezone, email_notifications, updated_at, valid_from, operation)
    VALUES (OLD.user_id, OLD.theme, OLD.language, OLD.timezone, OLD.email_notifications, OLD.updated_at, CURRENT_TIMESTAMP, 'D');
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_preferences_delete_trigger
    BEFORE DELETE ON user_preferences
    FOR EACH ROW
    EXECUTE FUNCTION user_preferences_delete_history();


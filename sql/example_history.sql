-- Generated History Tables and Triggers
-- This file contains history tables and triggers for temporal data tracking

-- History table and triggers for: users

CREATE TABLE users_history (
    id SERIAL,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D'))
);

CREATE INDEX idx_users_history_valid_from ON users_history (valid_from);
CREATE INDEX idx_users_history_valid_to ON users_history (valid_to);

-- Insert trigger for users
CREATE OR REPLACE FUNCTION users_insert_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO users_history (id, username, email, created_at, is_active, valid_from, operation)
    VALUES (NEW.id, NEW.username, NEW.email, NEW.created_at, NEW.is_active, CURRENT_TIMESTAMP, 'I');
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
    INSERT INTO users_history (id, username, email, created_at, is_active, valid_from, operation)
    VALUES (NEW.id, NEW.username, NEW.email, NEW.created_at, NEW.is_active, CURRENT_TIMESTAMP, 'U');
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
    UPDATE users_history SET valid_to = CURRENT_TIMESTAMP, operation = 'D'
    WHERE valid_to IS NULL AND id = OLD.id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_delete_trigger
    BEFORE DELETE ON users
    FOR EACH ROW
    EXECUTE FUNCTION users_delete_history();


--------------------------------------------------------------------------------

-- History table and triggers for: users

CREATE TABLE users_history (
    id SERIAL,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D'))
);

CREATE INDEX idx_users_history_valid_from ON users_history (valid_from);
CREATE INDEX idx_users_history_valid_to ON users_history (valid_to);

-- Insert trigger for users
CREATE OR REPLACE FUNCTION users_insert_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO users_history (id, username, email, created_at, is_active, valid_from, operation)
    VALUES (NEW.id, NEW.username, NEW.email, NEW.created_at, NEW.is_active, CURRENT_TIMESTAMP, 'I');
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
    INSERT INTO users_history (id, username, email, created_at, is_active, valid_from, operation)
    VALUES (NEW.id, NEW.username, NEW.email, NEW.created_at, NEW.is_active, CURRENT_TIMESTAMP, 'U');
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
    UPDATE users_history SET valid_to = CURRENT_TIMESTAMP, operation = 'D'
    WHERE valid_to IS NULL AND id = OLD.id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_delete_trigger
    BEFORE DELETE ON users
    FOR EACH ROW
    EXECUTE FUNCTION users_delete_history();


--------------------------------------------------------------------------------

-- History table and triggers for: products

CREATE TABLE products_history (
    product_id INTEGER,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    category_id INTEGER,
    in_stock BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D'))
);

CREATE INDEX idx_products_history_valid_from ON products_history (valid_from);
CREATE INDEX idx_products_history_valid_to ON products_history (valid_to);

-- Insert trigger for products
CREATE OR REPLACE FUNCTION products_insert_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO products_history (product_id, name, description, price, category_id, in_stock, valid_from, operation)
    VALUES (NEW.product_id, NEW.name, NEW.description, NEW.price, NEW.category_id, NEW.in_stock, CURRENT_TIMESTAMP, 'I');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER products_insert_trigger
    AFTER INSERT ON products
    FOR EACH ROW
    EXECUTE FUNCTION products_insert_history();

-- Update trigger for products
CREATE OR REPLACE FUNCTION products_update_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE products_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND product_id = OLD.product_id;
    INSERT INTO products_history (product_id, name, description, price, category_id, in_stock, valid_from, operation)
    VALUES (NEW.product_id, NEW.name, NEW.description, NEW.price, NEW.category_id, NEW.in_stock, CURRENT_TIMESTAMP, 'U');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER products_update_trigger
    AFTER UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION products_update_history();

-- Delete trigger for products
CREATE OR REPLACE FUNCTION products_delete_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE products_history SET valid_to = CURRENT_TIMESTAMP, operation = 'D'
    WHERE valid_to IS NULL AND product_id = OLD.product_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER products_delete_trigger
    BEFORE DELETE ON products
    FOR EACH ROW
    EXECUTE FUNCTION products_delete_history();


--------------------------------------------------------------------------------

-- History table and triggers for: orders

CREATE TABLE orders_history (
    order_id SERIAL,
    user_id INTEGER NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending',
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D'))
);

CREATE INDEX idx_orders_history_valid_from ON orders_history (valid_from);
CREATE INDEX idx_orders_history_valid_to ON orders_history (valid_to);

-- Insert trigger for orders
CREATE OR REPLACE FUNCTION orders_insert_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO orders_history (order_id, user_id, total_amount, order_date, status, valid_from, operation)
    VALUES (NEW.order_id, NEW.user_id, NEW.total_amount, NEW.order_date, NEW.status, CURRENT_TIMESTAMP, 'I');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_insert_trigger
    AFTER INSERT ON orders
    FOR EACH ROW
    EXECUTE FUNCTION orders_insert_history();

-- Update trigger for orders
CREATE OR REPLACE FUNCTION orders_update_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE orders_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND order_id = OLD.order_id;
    INSERT INTO orders_history (order_id, user_id, total_amount, order_date, status, valid_from, operation)
    VALUES (NEW.order_id, NEW.user_id, NEW.total_amount, NEW.order_date, NEW.status, CURRENT_TIMESTAMP, 'U');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_update_trigger
    AFTER UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION orders_update_history();

-- Delete trigger for orders
CREATE OR REPLACE FUNCTION orders_delete_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE orders_history SET valid_to = CURRENT_TIMESTAMP, operation = 'D'
    WHERE valid_to IS NULL AND order_id = OLD.order_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_delete_trigger
    BEFORE DELETE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION orders_delete_history();


-- Generated History Tables and Triggers
-- This file contains history tables and triggers for temporal data tracking

-- History table and triggers for: public.users

CREATE TABLE public.users_history (
    id SERIAL,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D'))
);

CREATE INDEX idx_public_users_history_valid_from ON public.users_history (valid_from);
CREATE INDEX idx_public_users_history_valid_to ON public.users_history (valid_to);

-- Insert trigger for public.users
CREATE OR REPLACE FUNCTION public_users_insert_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO public.users_history (id, username, email, created_at, is_active, valid_from, operation)
    VALUES (NEW.id, NEW.username, NEW.email, NEW.created_at, NEW.is_active, CURRENT_TIMESTAMP, 'I');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER public_users_insert_trigger
    AFTER INSERT ON public.users
    FOR EACH ROW
    EXECUTE FUNCTION public_users_insert_history();

-- Update trigger for public.users
CREATE OR REPLACE FUNCTION public_users_update_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE public.users_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND id = OLD.id;
    INSERT INTO public.users_history (id, username, email, created_at, is_active, valid_from, operation)
    VALUES (NEW.id, NEW.username, NEW.email, NEW.created_at, NEW.is_active, CURRENT_TIMESTAMP, 'U');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER public_users_update_trigger
    AFTER UPDATE ON public.users
    FOR EACH ROW
    EXECUTE FUNCTION public_users_update_history();

-- Delete trigger for public.users
CREATE OR REPLACE FUNCTION public_users_delete_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE public.users_history SET valid_to = CURRENT_TIMESTAMP, operation = 'D'
    WHERE valid_to IS NULL AND id = OLD.id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER public_users_delete_trigger
    BEFORE DELETE ON public.users
    FOR EACH ROW
    EXECUTE FUNCTION public_users_delete_history();

-- Example: Query public.users state at a specific point in time
-- Replace '2024-01-01 12:00:00' with your desired timestamp
SELECT * FROM public.users_history 
WHERE valid_from <= '2024-01-01 12:00:00' 
  AND (valid_to IS NULL OR valid_to > '2024-01-01 12:00:00')
  AND operation != 'D';

-- Example: Query public.users state as of now (current active records)
SELECT * FROM public.users_history 
WHERE valid_to IS NULL 
  AND operation != 'D';


--------------------------------------------------------------------------------

-- History table and triggers for: inventory.products

CREATE TABLE inventory.products_history (
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

CREATE INDEX idx_inventory_products_history_valid_from ON inventory.products_history (valid_from);
CREATE INDEX idx_inventory_products_history_valid_to ON inventory.products_history (valid_to);

-- Insert trigger for inventory.products
CREATE OR REPLACE FUNCTION inventory_products_insert_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO inventory.products_history (product_id, name, description, price, category_id, in_stock, valid_from, operation)
    VALUES (NEW.product_id, NEW.name, NEW.description, NEW.price, NEW.category_id, NEW.in_stock, CURRENT_TIMESTAMP, 'I');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER inventory_products_insert_trigger
    AFTER INSERT ON inventory.products
    FOR EACH ROW
    EXECUTE FUNCTION inventory_products_insert_history();

-- Update trigger for inventory.products
CREATE OR REPLACE FUNCTION inventory_products_update_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE inventory.products_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND product_id = OLD.product_id;
    INSERT INTO inventory.products_history (product_id, name, description, price, category_id, in_stock, valid_from, operation)
    VALUES (NEW.product_id, NEW.name, NEW.description, NEW.price, NEW.category_id, NEW.in_stock, CURRENT_TIMESTAMP, 'U');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER inventory_products_update_trigger
    AFTER UPDATE ON inventory.products
    FOR EACH ROW
    EXECUTE FUNCTION inventory_products_update_history();

-- Delete trigger for inventory.products
CREATE OR REPLACE FUNCTION inventory_products_delete_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE inventory.products_history SET valid_to = CURRENT_TIMESTAMP, operation = 'D'
    WHERE valid_to IS NULL AND product_id = OLD.product_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER inventory_products_delete_trigger
    BEFORE DELETE ON inventory.products
    FOR EACH ROW
    EXECUTE FUNCTION inventory_products_delete_history();

-- Example: Query inventory.products state at a specific point in time
-- Replace '2024-01-01 12:00:00' with your desired timestamp
SELECT * FROM inventory.products_history 
WHERE valid_from <= '2024-01-01 12:00:00' 
  AND (valid_to IS NULL OR valid_to > '2024-01-01 12:00:00')
  AND operation != 'D';

-- Example: Query inventory.products state as of now (current active records)
SELECT * FROM inventory.products_history 
WHERE valid_to IS NULL 
  AND operation != 'D';


--------------------------------------------------------------------------------

-- History table and triggers for: sales.orders

CREATE TABLE sales.orders_history (
    order_id SERIAL,
    user_id INTEGER NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending',
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D'))
);

CREATE INDEX idx_sales_orders_history_valid_from ON sales.orders_history (valid_from);
CREATE INDEX idx_sales_orders_history_valid_to ON sales.orders_history (valid_to);

-- Insert trigger for sales.orders
CREATE OR REPLACE FUNCTION sales_orders_insert_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO sales.orders_history (order_id, user_id, total_amount, order_date, status, valid_from, operation)
    VALUES (NEW.order_id, NEW.user_id, NEW.total_amount, NEW.order_date, NEW.status, CURRENT_TIMESTAMP, 'I');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sales_orders_insert_trigger
    AFTER INSERT ON sales.orders
    FOR EACH ROW
    EXECUTE FUNCTION sales_orders_insert_history();

-- Update trigger for sales.orders
CREATE OR REPLACE FUNCTION sales_orders_update_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE sales.orders_history SET valid_to = CURRENT_TIMESTAMP
    WHERE valid_to IS NULL AND order_id = OLD.order_id;
    INSERT INTO sales.orders_history (order_id, user_id, total_amount, order_date, status, valid_from, operation)
    VALUES (NEW.order_id, NEW.user_id, NEW.total_amount, NEW.order_date, NEW.status, CURRENT_TIMESTAMP, 'U');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sales_orders_update_trigger
    AFTER UPDATE ON sales.orders
    FOR EACH ROW
    EXECUTE FUNCTION sales_orders_update_history();

-- Delete trigger for sales.orders
CREATE OR REPLACE FUNCTION sales_orders_delete_history() RETURNS TRIGGER AS $$
BEGIN
    UPDATE sales.orders_history SET valid_to = CURRENT_TIMESTAMP, operation = 'D'
    WHERE valid_to IS NULL AND order_id = OLD.order_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sales_orders_delete_trigger
    BEFORE DELETE ON sales.orders
    FOR EACH ROW
    EXECUTE FUNCTION sales_orders_delete_history();

-- Example: Query sales.orders state at a specific point in time
-- Replace '2024-01-01 12:00:00' with your desired timestamp
SELECT * FROM sales.orders_history 
WHERE valid_from <= '2024-01-01 12:00:00' 
  AND (valid_to IS NULL OR valid_to > '2024-01-01 12:00:00')
  AND operation != 'D';

-- Example: Query sales.orders state as of now (current active records)
SELECT * FROM sales.orders_history 
WHERE valid_to IS NULL 
  AND operation != 'D';


-- Usage Examples:
-- 1. The history tables automatically track all changes via triggers
-- 2. Use the point-in-time queries above to view data as it existed at any timestamp
-- 3. The 'operation' column indicates: 'I'=Insert, 'U'=Update, 'D'=Delete
-- 4. valid_from shows when the record became active
-- 5. valid_to shows when the record was superseded (NULL = still active)

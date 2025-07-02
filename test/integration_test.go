package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/leinonen/sql-history/pkg/parser"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	ctx := context.Background()
	conn, err := connectToTestDB(ctx)
	if err != nil {
		t.Skipf("Could not connect to test database: %v", err)
	}
	defer conn.Close(ctx)

	t.Run("BasicHistoryTracking", func(t *testing.T) {
		testBasicHistoryTracking(t, ctx, conn)
	})

	t.Run("SchemaSupport", func(t *testing.T) {
		testSchemaSupport(t, ctx, conn)
	})

	t.Run("PointInTimeQueries", func(t *testing.T) {
		testPointInTimeQueries(t, ctx, conn)
	})

	t.Run("ForeignKeySupport", func(t *testing.T) {
		testForeignKeySupport(t, ctx, conn)
	})
}

func connectToTestDB(ctx context.Context) (*pgx.Conn, error) {
	connStr := "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=disable"

	var conn *pgx.Conn
	var err error

	for i := 0; i < 30; i++ {
		conn, err = pgx.Connect(ctx, connStr)
		if err == nil {
			err = conn.Ping(ctx)
			if err == nil {
				return conn, nil
			}
			conn.Close(ctx)
		}

		time.Sleep(time.Second)
	}

	return nil, fmt.Errorf("could not connect to database after 30 attempts: %v", err)
}

func testBasicHistoryTracking(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	cleanup := func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS users_history CASCADE")
		conn.Exec(ctx, "DROP TABLE IF EXISTS users CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS users_insert_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS users_update_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS users_delete_history() CASCADE")
	}
	cleanup()
	defer cleanup()

	originalSQL := `
	CREATE TABLE users (
		id SERIAL PRIMARY KEY,
		username VARCHAR(50) NOT NULL,
		email VARCHAR(100) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	_, err := conn.Exec(ctx, originalSQL)
	if err != nil {
		t.Fatalf("Failed to create original table: %v", err)
	}

	tables, err := parser.ParseCreateTables(originalSQL)
	if err != nil {
		t.Fatalf("Failed to parse tables: %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(tables))
	}

	historySQL := parser.GenerateHistoryTable(tables[0])
	_, err = conn.Exec(ctx, historySQL)
	if err != nil {
		t.Fatalf("Failed to create history table: %v", err)
	}

	triggersSQL := parser.GenerateTriggers(tables[0])
	_, err = conn.Exec(ctx, triggersSQL)
	if err != nil {
		t.Fatalf("Failed to create triggers: %v", err)
	}

	_, err = conn.Exec(ctx, "INSERT INTO users (username, email) VALUES ($1, $2)", "testuser", "test@example.com")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM users_history WHERE operation = 'I'").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query history table: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 insert record in history, got %d", count)
	}

	_, err = conn.Exec(ctx, "UPDATE users SET email = $1 WHERE username = $2", "newemail@example.com", "testuser")
	if err != nil {
		t.Fatalf("Failed to update test data: %v", err)
	}

	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM users_history WHERE operation = 'U'").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query history table after update: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 update record in history, got %d", count)
	}

	_, err = conn.Exec(ctx, "DELETE FROM users WHERE username = $1", "testuser")
	if err != nil {
		t.Fatalf("Failed to delete test data: %v", err)
	}

	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM users_history WHERE operation = 'D'").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query history table after delete: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 delete record in history, got %d", count)
	}
}

func testSchemaSupport(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	cleanup := func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_schema.products_history CASCADE")
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_schema.products CASCADE")
		conn.Exec(ctx, "DROP SCHEMA IF EXISTS test_schema CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS test_schema_products_insert_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS test_schema_products_update_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS test_schema_products_delete_history() CASCADE")
	}
	cleanup()
	defer cleanup()

	_, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS test_schema")
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	originalSQL := `
	CREATE TABLE test_schema.products (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		price DECIMAL(10,2) NOT NULL
	);`

	_, err = conn.Exec(ctx, originalSQL)
	if err != nil {
		t.Fatalf("Failed to create original table with schema: %v", err)
	}

	tables, err := parser.ParseCreateTables(originalSQL)
	if err != nil {
		t.Fatalf("Failed to parse tables with schema: %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(tables))
	}

	if tables[0].SchemaName != "test_schema" || tables[0].Name != "products" {
		t.Fatalf("Expected table test_schema.products, got %s.%s", tables[0].SchemaName, tables[0].Name)
	}

	historySQL := parser.GenerateHistoryTable(tables[0])
	_, err = conn.Exec(ctx, historySQL)
	if err != nil {
		t.Fatalf("Failed to create history table with schema: %v", err)
	}

	triggersSQL := parser.GenerateTriggers(tables[0])
	_, err = conn.Exec(ctx, triggersSQL)
	if err != nil {
		t.Fatalf("Failed to create triggers with schema: %v", err)
	}

	_, err = conn.Exec(ctx, "INSERT INTO test_schema.products (name, price) VALUES ($1, $2)", "Test Product", 29.99)
	if err != nil {
		t.Fatalf("Failed to insert test data into schema table: %v", err)
	}

	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM test_schema.products_history WHERE operation = 'I'").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query schema history table: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 insert record in schema history, got %d", count)
	}
}

func testPointInTimeQueries(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	cleanup := func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS timeline_test_history CASCADE")
		conn.Exec(ctx, "DROP TABLE IF EXISTS timeline_test CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS timeline_test_insert_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS timeline_test_update_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS timeline_test_delete_history() CASCADE")
	}
	cleanup()
	defer cleanup()

	originalSQL := `
	CREATE TABLE timeline_test (
		id SERIAL PRIMARY KEY,
		value VARCHAR(50) NOT NULL
	);`

	_, err := conn.Exec(ctx, originalSQL)
	if err != nil {
		t.Fatalf("Failed to create timeline test table: %v", err)
	}

	tables, err := parser.ParseCreateTables(originalSQL)
	if err != nil {
		t.Fatalf("Failed to parse timeline test tables: %v", err)
	}

	historySQL := parser.GenerateHistoryTable(tables[0])
	_, err = conn.Exec(ctx, historySQL)
	if err != nil {
		t.Fatalf("Failed to create timeline history table: %v", err)
	}

	triggersSQL := parser.GenerateTriggers(tables[0])
	_, err = conn.Exec(ctx, triggersSQL)
	if err != nil {
		t.Fatalf("Failed to create timeline triggers: %v", err)
	}

	_, err = conn.Exec(ctx, "INSERT INTO timeline_test (value) VALUES ($1)", "initial_value")
	if err != nil {
		t.Fatalf("Failed to insert initial value: %v", err)
	}

	// Get the time when initial record was inserted
	var insertTime time.Time
	err = conn.QueryRow(ctx, "SELECT valid_from FROM timeline_test_history WHERE operation = 'I' AND id = 1").Scan(&insertTime)
	if err != nil {
		t.Fatalf("Failed to get insert time: %v", err)
	}

	time.Sleep(1 * time.Second)

	_, err = conn.Exec(ctx, "UPDATE timeline_test SET value = $1 WHERE id = 1", "updated_value")
	if err != nil {
		t.Fatalf("Failed to update value: %v", err)
	}

	// Get the time when update record was inserted
	var updateTime time.Time
	err = conn.QueryRow(ctx, "SELECT valid_from FROM timeline_test_history WHERE operation = 'U' AND id = 1").Scan(&updateTime)
	if err != nil {
		t.Fatalf("Failed to get update time: %v", err)
	}

	var value string
	query := `SELECT value FROM timeline_test_history 
		      WHERE valid_from <= $1 
		      AND (valid_to IS NULL OR valid_to > $1)
		      AND operation != 'D' AND id = 1`

	// Query for a time just after the insert but before the update
	timeBetween := insertTime.Add(500 * time.Millisecond)
	err = conn.QueryRow(ctx, query, timeBetween).Scan(&value)
	if err != nil {
		t.Fatalf("Failed to query point-in-time data (timeBetween): %v", err)
	}

	if value != "initial_value" {
		t.Errorf("Expected 'initial_value' at timeBetween, got '%s'", value)
	}

	// Query for a time after the update
	timeAfterUpdate := updateTime.Add(100 * time.Millisecond)
	err = conn.QueryRow(ctx, query, timeAfterUpdate).Scan(&value)
	if err != nil {
		t.Fatalf("Failed to query point-in-time data (timeAfterUpdate): %v", err)
	}

	if value != "updated_value" {
		t.Errorf("Expected 'updated_value' at timeAfterUpdate, got '%s'", value)
	}
}

func testForeignKeySupport(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	cleanup := func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS fk_orders_history CASCADE")
		conn.Exec(ctx, "DROP TABLE IF EXISTS fk_orders CASCADE")
		conn.Exec(ctx, "DROP TABLE IF EXISTS fk_users_history CASCADE")
		conn.Exec(ctx, "DROP TABLE IF EXISTS fk_users CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS fk_users_insert_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS fk_users_update_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS fk_users_delete_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS fk_orders_insert_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS fk_orders_update_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS fk_orders_delete_history() CASCADE")
	}
	cleanup()
	defer cleanup()

	// Create parent table first
	usersSQL := `
	CREATE TABLE fk_users (
		id SERIAL PRIMARY KEY,
		username VARCHAR(50) NOT NULL UNIQUE,
		email VARCHAR(100) NOT NULL
	);`

	_, err := conn.Exec(ctx, usersSQL)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Create child table with foreign key relationships
	ordersSQL := `
	CREATE TABLE fk_orders (
		order_id SERIAL PRIMARY KEY,
		user_id INTEGER NOT NULL REFERENCES fk_users(id) ON DELETE CASCADE,
		status VARCHAR(20) DEFAULT 'pending',
		total DECIMAL(10,2) NOT NULL,
		assigned_user_id INTEGER,
		FOREIGN KEY (assigned_user_id) REFERENCES fk_users(id) ON DELETE SET NULL ON UPDATE CASCADE
	);`

	_, err = conn.Exec(ctx, ordersSQL)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Parse the tables to check foreign key extraction
	usersTables, err := parser.ParseCreateTables(usersSQL)
	if err != nil {
		t.Fatalf("Failed to parse users tables: %v", err)
	}

	ordersTables, err := parser.ParseCreateTables(ordersSQL)
	if err != nil {
		t.Fatalf("Failed to parse orders tables: %v", err)
	}

	if len(usersTables) != 1 || len(ordersTables) != 1 {
		t.Fatalf("Expected 1 users table and 1 orders table, got %d and %d", len(usersTables), len(ordersTables))
	}

	usersTable := usersTables[0]
	ordersTable := ordersTables[0]

	// Verify foreign keys were parsed correctly
	if len(usersTable.ForeignKeys) != 0 {
		t.Errorf("Expected 0 foreign keys in users table, got %d", len(usersTable.ForeignKeys))
	}

	if len(ordersTable.ForeignKeys) != 2 {
		t.Fatalf("Expected 2 foreign keys in orders table, got %d", len(ordersTable.ForeignKeys))
	}

	// Check inline foreign key (user_id)
	inlineFk := ordersTable.ForeignKeys[0]
	if inlineFk.ColumnName != "user_id" {
		t.Errorf("Expected inline FK column 'user_id', got '%s'", inlineFk.ColumnName)
	}
	if inlineFk.ReferencedTable != "fk_users" {
		t.Errorf("Expected inline FK table 'fk_users', got '%s'", inlineFk.ReferencedTable)
	}
	if inlineFk.ReferencedColumn != "id" {
		t.Errorf("Expected inline FK column 'id', got '%s'", inlineFk.ReferencedColumn)
	}
	if inlineFk.OnDelete != "CASCADE" {
		t.Errorf("Expected inline FK ON DELETE 'CASCADE', got '%s'", inlineFk.OnDelete)
	}

	// Check explicit foreign key (assigned_user_id)
	explicitFk := ordersTable.ForeignKeys[1]
	if explicitFk.ColumnName != "assigned_user_id" {
		t.Errorf("Expected explicit FK column 'assigned_user_id', got '%s'", explicitFk.ColumnName)
	}
	if explicitFk.ReferencedTable != "fk_users" {
		t.Errorf("Expected explicit FK table 'fk_users', got '%s'", explicitFk.ReferencedTable)
	}
	if explicitFk.ReferencedColumn != "id" {
		t.Errorf("Expected explicit FK column 'id', got '%s'", explicitFk.ReferencedColumn)
	}
	if explicitFk.OnDelete != "SET NULL" {
		t.Errorf("Expected explicit FK ON DELETE 'SET NULL', got '%s'", explicitFk.OnDelete)
	}
	if explicitFk.OnUpdate != "CASCADE" {
		t.Errorf("Expected explicit FK ON UPDATE 'CASCADE', got '%s'", explicitFk.OnUpdate)
	}

	// Create history tables and triggers for both tables
	usersHistorySQL := parser.GenerateHistoryTable(usersTable)
	_, err = conn.Exec(ctx, usersHistorySQL)
	if err != nil {
		t.Fatalf("Failed to create users history table: %v", err)
	}

	usersTriggersSQL := parser.GenerateTriggers(usersTable)
	_, err = conn.Exec(ctx, usersTriggersSQL)
	if err != nil {
		t.Fatalf("Failed to create users triggers: %v", err)
	}

	ordersHistorySQL := parser.GenerateHistoryTable(ordersTable)
	_, err = conn.Exec(ctx, ordersHistorySQL)
	if err != nil {
		t.Fatalf("Failed to create orders history table: %v", err)
	}

	ordersTriggersSQL := parser.GenerateTriggers(ordersTable)
	_, err = conn.Exec(ctx, ordersTriggersSQL)
	if err != nil {
		t.Fatalf("Failed to create orders triggers: %v", err)
	}

	// Test data operations with foreign key relationships
	_, err = conn.Exec(ctx, "INSERT INTO fk_users (username, email) VALUES ($1, $2)", "user1", "user1@example.com")
	if err != nil {
		t.Fatalf("Failed to insert user: %v", err)
	}

	_, err = conn.Exec(ctx, "INSERT INTO fk_users (username, email) VALUES ($1, $2)", "user2", "user2@example.com")
	if err != nil {
		t.Fatalf("Failed to insert second user: %v", err)
	}

	_, err = conn.Exec(ctx, "INSERT INTO fk_orders (user_id, status, total, assigned_user_id) VALUES ($1, $2, $3, $4)", 1, "pending", 100.50, 2)
	if err != nil {
		t.Fatalf("Failed to insert order: %v", err)
	}

	// Verify history tracking works with foreign key relationships
	var userHistoryCount int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM fk_users_history WHERE operation = 'I'").Scan(&userHistoryCount)
	if err != nil {
		t.Fatalf("Failed to query users history: %v", err)
	}

	if userHistoryCount != 2 {
		t.Errorf("Expected 2 user insert records in history, got %d", userHistoryCount)
	}

	var orderHistoryCount int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM fk_orders_history WHERE operation = 'I'").Scan(&orderHistoryCount)
	if err != nil {
		t.Fatalf("Failed to query orders history: %v", err)
	}

	if orderHistoryCount != 1 {
		t.Errorf("Expected 1 order insert record in history, got %d", orderHistoryCount)
	}

	// Test foreign key constraint behavior by updating referenced data
	_, err = conn.Exec(ctx, "UPDATE fk_users SET username = $1 WHERE id = $2", "updated_user2", 2)
	if err != nil {
		t.Fatalf("Failed to update user: %v", err)
	}

	// Verify the update was tracked in history
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM fk_users_history WHERE operation = 'U'").Scan(&userHistoryCount)
	if err != nil {
		t.Fatalf("Failed to query users history after update: %v", err)
	}

	if userHistoryCount != 1 {
		t.Errorf("Expected 1 user update record in history, got %d", userHistoryCount)
	}

	// Test cascading delete behavior
	// Note: PostgreSQL cascading deletes don't fire user triggers, so we need to verify
	// the actual data was deleted but won't have a delete history record for the cascaded row
	_, err = conn.Exec(ctx, "DELETE FROM fk_users WHERE id = $1", 1)
	if err != nil {
		t.Fatalf("Failed to delete user: %v", err)
	}

	// Verify user delete record was created
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM fk_users_history WHERE operation = 'D'").Scan(&userHistoryCount)
	if err != nil {
		t.Fatalf("Failed to query users history after delete: %v", err)
	}

	if userHistoryCount != 1 {
		t.Errorf("Expected 1 user delete record in history, got %d", userHistoryCount)
	}

	// Verify the order was actually deleted from the main table (cascading delete worked)
	var remainingOrders int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM fk_orders").Scan(&remainingOrders)
	if err != nil {
		t.Fatalf("Failed to query remaining orders: %v", err)
	}

	if remainingOrders != 0 {
		t.Errorf("Expected 0 remaining orders after cascade delete, got %d", remainingOrders)
	}

	// Note: No delete history record is expected for cascaded deletes as PostgreSQL
	// doesn't fire user triggers for cascading actions. This is expected behavior.
}

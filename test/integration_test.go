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

	t.Run("UserTrackingCurrentUser", func(t *testing.T) {
		testUserTrackingCurrentUser(t, ctx, conn)
	})

	t.Run("UserTrackingSession", func(t *testing.T) {
		testUserTrackingSession(t, ctx, conn)
	})

	t.Run("UserTrackingWithSchema", func(t *testing.T) {
		testUserTrackingWithSchema(t, ctx, conn)
	})

	t.Run("UserTrackingWithForeignKeys", func(t *testing.T) {
		testUserTrackingWithForeignKeys(t, ctx, conn)
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

	config := parser.Config{TrackUser: false, UserSource: "current_user"}
	historySQL := parser.GenerateHistoryTable(tables[0], config)
	_, err = conn.Exec(ctx, historySQL)
	if err != nil {
		t.Fatalf("Failed to create history table: %v", err)
	}

	triggersSQL := parser.GenerateTriggers(tables[0], config)
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

	config := parser.Config{TrackUser: false, UserSource: "current_user"}
	historySQL := parser.GenerateHistoryTable(tables[0], config)
	_, err = conn.Exec(ctx, historySQL)
	if err != nil {
		t.Fatalf("Failed to create history table with schema: %v", err)
	}

	triggersSQL := parser.GenerateTriggers(tables[0], config)
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

	config := parser.Config{TrackUser: false, UserSource: "current_user"}
	historySQL := parser.GenerateHistoryTable(tables[0], config)
	_, err = conn.Exec(ctx, historySQL)
	if err != nil {
		t.Fatalf("Failed to create timeline history table: %v", err)
	}

	triggersSQL := parser.GenerateTriggers(tables[0], config)
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
	config := parser.Config{TrackUser: false, UserSource: "current_user"}
	usersHistorySQL := parser.GenerateHistoryTable(usersTable, config)
	_, err = conn.Exec(ctx, usersHistorySQL)
	if err != nil {
		t.Fatalf("Failed to create users history table: %v", err)
	}

	usersTriggersSQL := parser.GenerateTriggers(usersTable, config)
	_, err = conn.Exec(ctx, usersTriggersSQL)
	if err != nil {
		t.Fatalf("Failed to create users triggers: %v", err)
	}

	ordersHistorySQL := parser.GenerateHistoryTable(ordersTable, config)
	_, err = conn.Exec(ctx, ordersHistorySQL)
	if err != nil {
		t.Fatalf("Failed to create orders history table: %v", err)
	}

	ordersTriggersSQL := parser.GenerateTriggers(ordersTable, config)
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

func testUserTrackingCurrentUser(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	cleanup := func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS user_tracking_test_history CASCADE")
		conn.Exec(ctx, "DROP TABLE IF EXISTS user_tracking_test CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS user_tracking_test_insert_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS user_tracking_test_update_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS user_tracking_test_delete_history() CASCADE")
	}
	cleanup()
	defer cleanup()

	originalSQL := `
	CREATE TABLE user_tracking_test (
		id SERIAL PRIMARY KEY,
		name VARCHAR(50) NOT NULL,
		value INTEGER NOT NULL
	);`

	_, err := conn.Exec(ctx, originalSQL)
	if err != nil {
		t.Fatalf("Failed to create user tracking test table: %v", err)
	}

	tables, err := parser.ParseCreateTables(originalSQL)
	if err != nil {
		t.Fatalf("Failed to parse user tracking test tables: %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(tables))
	}

	// Test with user tracking enabled using current_user
	config := parser.Config{TrackUser: true, UserSource: "current_user"}
	historySQL := parser.GenerateHistoryTable(tables[0], config)
	_, err = conn.Exec(ctx, historySQL)
	if err != nil {
		t.Fatalf("Failed to create user tracking history table: %v", err)
	}

	triggersSQL := parser.GenerateTriggers(tables[0], config)
	t.Logf("Generated triggers SQL:\n%s", triggersSQL)
	_, err = conn.Exec(ctx, triggersSQL)
	if err != nil {
		t.Fatalf("Failed to create user tracking triggers: %v", err)
	}

	// Insert test data
	_, err = conn.Exec(ctx, "INSERT INTO user_tracking_test (name, value) VALUES ($1, $2)", "test_record", 42)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Verify insert was tracked with user
	var insertUser string
	var operation string
	err = conn.QueryRow(ctx, "SELECT changed_by, operation FROM user_tracking_test_history WHERE operation = 'I'").Scan(&insertUser, &operation)
	if err != nil {
		t.Fatalf("Failed to query insert tracking: %v", err)
	}

	if operation != "I" {
		t.Errorf("Expected operation 'I', got '%s'", operation)
	}

	if insertUser == "" {
		t.Error("Expected changed_by to be populated for insert")
	}

	// Update test data
	result, err := conn.Exec(ctx, "UPDATE user_tracking_test SET value = $1 WHERE name = $2", 84, "test_record")
	if err != nil {
		t.Fatalf("Failed to update test data: %v", err)
	}
	rowsAffected := result.RowsAffected()
	t.Logf("UPDATE affected %d rows", rowsAffected)
	
	if rowsAffected == 0 {
		t.Fatal("UPDATE affected 0 rows - this suggests the WHERE clause didn't match any records")
	}

	// Debug: Check records after UPDATE
	var recordCountAfterUpdate int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM user_tracking_test_history").Scan(&recordCountAfterUpdate)
	if err != nil {
		t.Fatalf("Failed to count records after update: %v", err)
	}
	t.Logf("Records in history after UPDATE: %d", recordCountAfterUpdate)
	
	// Debug: Show all records with their details after UPDATE
	rows2, err := conn.Query(ctx, "SELECT operation, changed_by, COALESCE(valid_to::text, 'NULL') FROM user_tracking_test_history ORDER BY valid_from")
	if err != nil {
		t.Fatalf("Failed to query all records after update: %v", err)
	}
	defer rows2.Close()
	
	recordCount2 := 0
	for rows2.Next() {
		var op, user, validToStr string
		if err := rows2.Scan(&op, &user, &validToStr); err != nil {
			t.Fatalf("Failed to scan record after update: %v", err)
		}
		t.Logf("After UPDATE - Record %d: operation=%s, changed_by=%s, valid_to=%s", recordCount2+1, op, user, validToStr)
		recordCount2++
	}

	// Verify update was tracked with user
	var updateUser string
	err = conn.QueryRow(ctx, "SELECT changed_by FROM user_tracking_test_history WHERE operation = 'U'").Scan(&updateUser)
	if err != nil {
		t.Fatalf("Failed to query update tracking: %v", err)
	}

	if updateUser == "" {
		t.Error("Expected changed_by to be populated for update")
	}

	if insertUser != updateUser {
		t.Errorf("Expected same user for insert and update, got '%s' and '%s'", insertUser, updateUser)
	}

	// Delete test data
	_, err = conn.Exec(ctx, "DELETE FROM user_tracking_test WHERE name = $1", "test_record")
	if err != nil {
		t.Fatalf("Failed to delete test data: %v", err)
	}

	// Verify delete was tracked with user
	var deleteUser string
	err = conn.QueryRow(ctx, "SELECT changed_by FROM user_tracking_test_history WHERE operation = 'D'").Scan(&deleteUser)
	if err != nil {
		t.Fatalf("Failed to query delete tracking: %v", err)
	}

	if deleteUser == "" {
		t.Error("Expected changed_by to be populated for delete")
	}

	if insertUser != deleteUser {
		t.Errorf("Expected same user for insert and delete, got '%s' and '%s'", insertUser, deleteUser)
	}

	// Debug: let's see what records we actually have
	rows, err := conn.Query(ctx, "SELECT operation, changed_by FROM user_tracking_test_history ORDER BY valid_from")
	if err != nil {
		t.Fatalf("Failed to query all records: %v", err)
	}
	defer rows.Close()
	
	recordCount := 0
	for rows.Next() {
		var op, user string
		if err := rows.Scan(&op, &user); err != nil {
			t.Fatalf("Failed to scan record: %v", err)
		}
		t.Logf("Record %d: operation=%s, changed_by=%s", recordCount+1, op, user)
		recordCount++
	}
	
	// Verify all operations have consistent user tracking
	var insertCount, updateCount, deleteCount int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM user_tracking_test_history WHERE operation = 'I' AND changed_by = $1", insertUser).Scan(&insertCount)
	if err != nil {
		t.Fatalf("Failed to count insert records: %v", err)
	}
	
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM user_tracking_test_history WHERE operation = 'U' AND changed_by = $1", insertUser).Scan(&updateCount)
	if err != nil {
		t.Fatalf("Failed to count update records: %v", err)
	}
	
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM user_tracking_test_history WHERE operation = 'D' AND changed_by = $1", insertUser).Scan(&deleteCount)
	if err != nil {
		t.Fatalf("Failed to count delete records: %v", err)
	}

	if insertCount != 1 {
		t.Errorf("Expected 1 insert record, got %d", insertCount)
	}
	if updateCount != 1 {
		t.Errorf("Expected 1 update record, got %d", updateCount)
	}
	if deleteCount != 1 {
		t.Errorf("Expected 1 delete record, got %d", deleteCount)
	}
}

func testUserTrackingSession(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	cleanup := func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS session_tracking_test_history CASCADE")
		conn.Exec(ctx, "DROP TABLE IF EXISTS session_tracking_test CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS session_tracking_test_insert_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS session_tracking_test_update_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS session_tracking_test_delete_history() CASCADE")
	}
	cleanup()
	defer cleanup()

	originalSQL := `
	CREATE TABLE session_tracking_test (
		id SERIAL PRIMARY KEY,
		name VARCHAR(50) NOT NULL,
		description TEXT
	);`

	_, err := conn.Exec(ctx, originalSQL)
	if err != nil {
		t.Fatalf("Failed to create session tracking test table: %v", err)
	}

	tables, err := parser.ParseCreateTables(originalSQL)
	if err != nil {
		t.Fatalf("Failed to parse session tracking test tables: %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(tables))
	}

	// Test with user tracking enabled using session variables
	config := parser.Config{TrackUser: true, UserSource: "session"}
	historySQL := parser.GenerateHistoryTable(tables[0], config)
	_, err = conn.Exec(ctx, historySQL)
	if err != nil {
		t.Fatalf("Failed to create session tracking history table: %v", err)
	}

	triggersSQL := parser.GenerateTriggers(tables[0], config)
	_, err = conn.Exec(ctx, triggersSQL)
	if err != nil {
		t.Fatalf("Failed to create session tracking triggers: %v", err)
	}

	// Test with no session variable set (should fall back to current_user)
	_, err = conn.Exec(ctx, "INSERT INTO session_tracking_test (name, description) VALUES ($1, $2)", "fallback_test", "Should use current_user")
	if err != nil {
		t.Fatalf("Failed to insert fallback test data: %v", err)
	}

	var fallbackUser string
	err = conn.QueryRow(ctx, "SELECT changed_by FROM session_tracking_test_history WHERE name = 'fallback_test'").Scan(&fallbackUser)
	if err != nil {
		t.Fatalf("Failed to query fallback tracking: %v", err)
	}

	if fallbackUser == "" {
		t.Error("Expected changed_by to be populated with fallback user")
	}

	// Set session variable and test
	_, err = conn.Exec(ctx, "SELECT set_config('app.current_user', 'john.doe@company.com', false)")
	if err != nil {
		t.Fatalf("Failed to set session variable: %v", err)
	}

	_, err = conn.Exec(ctx, "INSERT INTO session_tracking_test (name, description) VALUES ($1, $2)", "session_test", "Should use session variable")
	if err != nil {
		t.Fatalf("Failed to insert session test data: %v", err)
	}

	var sessionUser string
	err = conn.QueryRow(ctx, "SELECT changed_by FROM session_tracking_test_history WHERE name = 'session_test'").Scan(&sessionUser)
	if err != nil {
		t.Fatalf("Failed to query session tracking: %v", err)
	}

	if sessionUser != "john.doe@company.com" {
		t.Errorf("Expected changed_by to be 'john.doe@company.com', got '%s'", sessionUser)
	}

	// Change session user and test update
	_, err = conn.Exec(ctx, "SELECT set_config('app.current_user', 'jane.smith@company.com', false)")
	if err != nil {
		t.Fatalf("Failed to change session variable: %v", err)
	}

	_, err = conn.Exec(ctx, "UPDATE session_tracking_test SET description = $1 WHERE name = $2", "Updated by Jane", "session_test")
	if err != nil {
		t.Fatalf("Failed to update session test data: %v", err)
	}

	var updateUser string
	err = conn.QueryRow(ctx, "SELECT changed_by FROM session_tracking_test_history WHERE operation = 'U'").Scan(&updateUser)
	if err != nil {
		t.Fatalf("Failed to query session update tracking: %v", err)
	}

	if updateUser != "jane.smith@company.com" {
		t.Errorf("Expected update changed_by to be 'jane.smith@company.com', got '%s'", updateUser)
	}

	// Test delete with different user
	_, err = conn.Exec(ctx, "SELECT set_config('app.current_user', 'admin@company.com', false)")
	if err != nil {
		t.Fatalf("Failed to change session variable for delete: %v", err)
	}

	_, err = conn.Exec(ctx, "DELETE FROM session_tracking_test WHERE name = $1", "session_test")
	if err != nil {
		t.Fatalf("Failed to delete session test data: %v", err)
	}

	var deleteUser string
	err = conn.QueryRow(ctx, "SELECT changed_by FROM session_tracking_test_history WHERE operation = 'D'").Scan(&deleteUser)
	if err != nil {
		t.Fatalf("Failed to query session delete tracking: %v", err)
	}

	if deleteUser != "admin@company.com" {
		t.Errorf("Expected delete changed_by to be 'admin@company.com', got '%s'", deleteUser)
	}

	// Verify we have different users for different operations
	var distinctUsers int
	err = conn.QueryRow(ctx, "SELECT COUNT(DISTINCT changed_by) FROM session_tracking_test_history").Scan(&distinctUsers)
	if err != nil {
		t.Fatalf("Failed to count distinct users: %v", err)
	}

	if distinctUsers < 3 {
		t.Errorf("Expected at least 3 distinct users in tracking, got %d", distinctUsers)
	}
}

func testUserTrackingWithSchema(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	cleanup := func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS tracking_schema.schema_test_history CASCADE")
		conn.Exec(ctx, "DROP TABLE IF EXISTS tracking_schema.schema_test CASCADE")
		conn.Exec(ctx, "DROP SCHEMA IF EXISTS tracking_schema CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS tracking_schema_schema_test_insert_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS tracking_schema_schema_test_update_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS tracking_schema_schema_test_delete_history() CASCADE")
	}
	cleanup()
	defer cleanup()

	_, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS tracking_schema")
	if err != nil {
		t.Fatalf("Failed to create tracking schema: %v", err)
	}

	originalSQL := `
	CREATE TABLE tracking_schema.schema_test (
		id SERIAL PRIMARY KEY,
		data VARCHAR(100) NOT NULL,
		status VARCHAR(20) DEFAULT 'active'
	);`

	_, err = conn.Exec(ctx, originalSQL)
	if err != nil {
		t.Fatalf("Failed to create schema table: %v", err)
	}

	tables, err := parser.ParseCreateTables(originalSQL)
	if err != nil {
		t.Fatalf("Failed to parse schema tables: %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(tables))
	}

	if tables[0].SchemaName != "tracking_schema" || tables[0].Name != "schema_test" {
		t.Fatalf("Expected table tracking_schema.schema_test, got %s.%s", tables[0].SchemaName, tables[0].Name)
	}

	// Test user tracking with schema-qualified tables
	config := parser.Config{TrackUser: true, UserSource: "current_user"}
	historySQL := parser.GenerateHistoryTable(tables[0], config)
	_, err = conn.Exec(ctx, historySQL)
	if err != nil {
		t.Fatalf("Failed to create schema history table: %v", err)
	}

	triggersSQL := parser.GenerateTriggers(tables[0], config)
	_, err = conn.Exec(ctx, triggersSQL)
	if err != nil {
		t.Fatalf("Failed to create schema triggers: %v", err)
	}

	// Insert data and verify user tracking works with schema
	_, err = conn.Exec(ctx, "INSERT INTO tracking_schema.schema_test (data, status) VALUES ($1, $2)", "schema_data", "active")
	if err != nil {
		t.Fatalf("Failed to insert schema test data: %v", err)
	}

	var changedBy string
	var operation string
	err = conn.QueryRow(ctx, "SELECT changed_by, operation FROM tracking_schema.schema_test_history WHERE operation = 'I'").Scan(&changedBy, &operation)
	if err != nil {
		t.Fatalf("Failed to query schema tracking: %v", err)
	}

	if operation != "I" {
		t.Errorf("Expected operation 'I', got '%s'", operation)
	}

	if changedBy == "" {
		t.Error("Expected changed_by to be populated for schema table")
	}

	// Update and verify
	_, err = conn.Exec(ctx, "UPDATE tracking_schema.schema_test SET status = $1 WHERE data = $2", "updated", "schema_data")
	if err != nil {
		t.Fatalf("Failed to update schema test data: %v", err)
	}

	var updateCount int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM tracking_schema.schema_test_history WHERE operation = 'U' AND changed_by = $1", changedBy).Scan(&updateCount)
	if err != nil {
		t.Fatalf("Failed to count schema updates: %v", err)
	}

	if updateCount != 1 {
		t.Errorf("Expected 1 update record with user tracking in schema, got %d", updateCount)
	}
}

func testUserTrackingWithForeignKeys(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	cleanup := func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS fk_user_orders_history CASCADE")
		conn.Exec(ctx, "DROP TABLE IF EXISTS fk_user_orders CASCADE")
		conn.Exec(ctx, "DROP TABLE IF EXISTS fk_user_customers_history CASCADE")
		conn.Exec(ctx, "DROP TABLE IF EXISTS fk_user_customers CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS fk_user_customers_insert_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS fk_user_customers_update_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS fk_user_customers_delete_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS fk_user_orders_insert_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS fk_user_orders_update_history() CASCADE")
		conn.Exec(ctx, "DROP FUNCTION IF EXISTS fk_user_orders_delete_history() CASCADE")
	}
	cleanup()
	defer cleanup()

	// Create parent table
	customersSQL := `
	CREATE TABLE fk_user_customers (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		email VARCHAR(100) NOT NULL UNIQUE
	);`

	_, err := conn.Exec(ctx, customersSQL)
	if err != nil {
		t.Fatalf("Failed to create customers table: %v", err)
	}

	// Create child table with foreign key
	ordersSQL := `
	CREATE TABLE fk_user_orders (
		order_id SERIAL PRIMARY KEY,
		customer_id INTEGER NOT NULL REFERENCES fk_user_customers(id) ON DELETE CASCADE,
		amount DECIMAL(10,2) NOT NULL,
		order_date DATE DEFAULT CURRENT_DATE
	);`

	_, err = conn.Exec(ctx, ordersSQL)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Parse tables
	customersTables, err := parser.ParseCreateTables(customersSQL)
	if err != nil {
		t.Fatalf("Failed to parse customers tables: %v", err)
	}

	ordersTables, err := parser.ParseCreateTables(ordersSQL)
	if err != nil {
		t.Fatalf("Failed to parse orders tables: %v", err)
	}

	if len(customersTables) != 1 || len(ordersTables) != 1 {
		t.Fatalf("Expected 1 customers table and 1 orders table, got %d and %d", len(customersTables), len(ordersTables))
	}

	customersTable := customersTables[0]
	ordersTable := ordersTables[0]

	// Create history tables and triggers with user tracking
	config := parser.Config{TrackUser: true, UserSource: "session"}

	customersHistorySQL := parser.GenerateHistoryTable(customersTable, config)
	_, err = conn.Exec(ctx, customersHistorySQL)
	if err != nil {
		t.Fatalf("Failed to create customers history table: %v", err)
	}

	customersTriggersSQL := parser.GenerateTriggers(customersTable, config)
	_, err = conn.Exec(ctx, customersTriggersSQL)
	if err != nil {
		t.Fatalf("Failed to create customers triggers: %v", err)
	}

	ordersHistorySQL := parser.GenerateHistoryTable(ordersTable, config)
	_, err = conn.Exec(ctx, ordersHistorySQL)
	if err != nil {
		t.Fatalf("Failed to create orders history table: %v", err)
	}

	ordersTriggersSQL := parser.GenerateTriggers(ordersTable, config)
	_, err = conn.Exec(ctx, ordersTriggersSQL)
	if err != nil {
		t.Fatalf("Failed to create orders triggers: %v", err)
	}

	// Set session user for testing
	_, err = conn.Exec(ctx, "SELECT set_config('app.current_user', 'sales.rep@company.com', false)")
	if err != nil {
		t.Fatalf("Failed to set session user: %v", err)
	}

	// Insert parent record
	_, err = conn.Exec(ctx, "INSERT INTO fk_user_customers (name, email) VALUES ($1, $2)", "John Customer", "john@customer.com")
	if err != nil {
		t.Fatalf("Failed to insert customer: %v", err)
	}

	// Insert child record with foreign key relationship
	_, err = conn.Exec(ctx, "INSERT INTO fk_user_orders (customer_id, amount) VALUES ($1, $2)", 1, 299.99)
	if err != nil {
		t.Fatalf("Failed to insert order: %v", err)
	}

	// Verify both inserts were tracked with the same user
	var customerUser string
	err = conn.QueryRow(ctx, "SELECT changed_by FROM fk_user_customers_history WHERE operation = 'I'").Scan(&customerUser)
	if err != nil {
		t.Fatalf("Failed to query customer tracking: %v", err)
	}

	var orderUser string
	err = conn.QueryRow(ctx, "SELECT changed_by FROM fk_user_orders_history WHERE operation = 'I'").Scan(&orderUser)
	if err != nil {
		t.Fatalf("Failed to query order tracking: %v", err)
	}

	if customerUser != "sales.rep@company.com" {
		t.Errorf("Expected customer changed_by to be 'sales.rep@company.com', got '%s'", customerUser)
	}

	if orderUser != "sales.rep@company.com" {
		t.Errorf("Expected order changed_by to be 'sales.rep@company.com', got '%s'", orderUser)
	}

	// Change user and update order
	_, err = conn.Exec(ctx, "SELECT set_config('app.current_user', 'manager@company.com', false)")
	if err != nil {
		t.Fatalf("Failed to change session user: %v", err)
	}

	_, err = conn.Exec(ctx, "UPDATE fk_user_orders SET amount = $1 WHERE order_id = $2", 399.99, 1)
	if err != nil {
		t.Fatalf("Failed to update order: %v", err)
	}

	// Verify update was tracked with new user
	var updateUser string
	err = conn.QueryRow(ctx, "SELECT changed_by FROM fk_user_orders_history WHERE operation = 'U'").Scan(&updateUser)
	if err != nil {
		t.Fatalf("Failed to query order update tracking: %v", err)
	}

	if updateUser != "manager@company.com" {
		t.Errorf("Expected order update changed_by to be 'manager@company.com', got '%s'", updateUser)
	}

	// Test cascading delete with user tracking
	_, err = conn.Exec(ctx, "SELECT set_config('app.current_user', 'admin@company.com', false)")
	if err != nil {
		t.Fatalf("Failed to change session user for delete: %v", err)
	}

	_, err = conn.Exec(ctx, "DELETE FROM fk_user_customers WHERE id = $1", 1)
	if err != nil {
		t.Fatalf("Failed to delete customer: %v", err)
	}

	// Verify customer delete was tracked
	var deleteUser string
	err = conn.QueryRow(ctx, "SELECT changed_by FROM fk_user_customers_history WHERE operation = 'D'").Scan(&deleteUser)
	if err != nil {
		t.Fatalf("Failed to query customer delete tracking: %v", err)
	}

	if deleteUser != "admin@company.com" {
		t.Errorf("Expected customer delete changed_by to be 'admin@company.com', got '%s'", deleteUser)
	}

	// Verify the order was cascaded deleted from main table
	var remainingOrders int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM fk_user_orders").Scan(&remainingOrders)
	if err != nil {
		t.Fatalf("Failed to count remaining orders: %v", err)
	}

	if remainingOrders != 0 {
		t.Errorf("Expected 0 remaining orders after cascade delete, got %d", remainingOrders)
	}

	// Note: Cascaded deletes don't trigger user triggers in PostgreSQL, so we won't have
	// a delete history record for the order. This is expected behavior.
}

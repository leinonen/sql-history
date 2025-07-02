package parser

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestParseCreateTables(t *testing.T) {
	sqlContent := `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(50) NOT NULL UNIQUE,
			email VARCHAR(100) NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE TABLE products (
			product_id INTEGER PRIMARY KEY,
			name VARCHAR(200) NOT NULL,
			price DECIMAL(10,2) NOT NULL
		);
	`

	tables, err := ParseCreateTables(sqlContent)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if len(tables) != 2 {
		t.Fatalf("Expected 2 tables, got: %d", len(tables))
	}

	if tables[0].Name != "users" {
		t.Errorf("Expected first table name 'users', got: %s", tables[0].Name)
	}

	if tables[1].Name != "products" {
		t.Errorf("Expected second table name 'products', got: %s", tables[1].Name)
	}

	if len(tables[0].Columns) < 2 {
		t.Errorf("Expected at least 2 columns in users table, got: %d", len(tables[0].Columns))
	}
}

func TestParseCreateTablesWithSchema(t *testing.T) {
	sqlContent := `
		CREATE TABLE public.users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(50) NOT NULL
		);
		
		CREATE TABLE inventory.products (
			product_id INTEGER PRIMARY KEY,
			name VARCHAR(200) NOT NULL
		);
	`

	tables, err := ParseCreateTables(sqlContent)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if len(tables) != 2 {
		t.Fatalf("Expected 2 tables, got: %d", len(tables))
	}

	if tables[0].SchemaName != "public" || tables[0].Name != "users" {
		t.Errorf("Expected first table 'public.users', got: '%s.%s'", tables[0].SchemaName, tables[0].Name)
	}

	if tables[1].SchemaName != "inventory" || tables[1].Name != "products" {
		t.Errorf("Expected second table 'inventory.products', got: '%s.%s'", tables[1].SchemaName, tables[1].Name)
	}
}

func TestParseColumns(t *testing.T) {
	columnsStr := "id SERIAL PRIMARY KEY, username VARCHAR(50) NOT NULL, price DECIMAL(10,2)"

	columns, foreignKeys, err := ParseColumns(columnsStr)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if len(columns) != 3 {
		t.Fatalf("Expected 3 columns, got: %d", len(columns))
	}

	if len(foreignKeys) != 0 {
		t.Fatalf("Expected 0 foreign keys, got: %d", len(foreignKeys))
	}

	expectedCols := []struct {
		name     string
		dataType string
		options  string
	}{
		{"id", "SERIAL", "PRIMARY KEY"},
		{"username", "VARCHAR(50)", "NOT NULL"},
		{"price", "DECIMAL(10,2)", ""},
	}

	for i, expected := range expectedCols {
		if columns[i].Name != expected.name {
			t.Errorf("Expected column %d name '%s', got: '%s'", i, expected.name, columns[i].Name)
		}
		if columns[i].DataType != expected.dataType {
			t.Errorf("Expected column %d dataType '%s', got: '%s'", i, expected.dataType, columns[i].DataType)
		}
		if columns[i].Options != expected.options {
			t.Errorf("Expected column %d options '%s', got: '%s'", i, expected.options, columns[i].Options)
		}
	}
}

func TestGenerateHistoryTable(t *testing.T) {
	table := Table{
		Name: "users",
		Columns: []Column{
			{Name: "id", DataType: "SERIAL", Options: "PRIMARY KEY"},
			{Name: "username", DataType: "VARCHAR(50)", Options: "NOT NULL"},
		},
	}

	result := GenerateHistoryTable(table)

	expectedContains := []string{
		"CREATE TABLE users_history",
		"id SERIAL",
		"username VARCHAR(50) NOT NULL",
		"valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
		"valid_to TIMESTAMP NULL",
		"operation CHAR(1) NOT NULL",
		"CREATE INDEX idx_users_history_valid_from",
		"CREATE INDEX idx_users_history_valid_to",
	}

	for _, expected := range expectedContains {
		if !strings.Contains(result, expected) {
			t.Errorf("Expected result to contain '%s', but it didn't", expected)
		}
	}
}

func TestGenerateTriggers(t *testing.T) {
	table := Table{
		Name: "users",
		Columns: []Column{
			{Name: "id", DataType: "SERIAL", Options: "PRIMARY KEY"},
			{Name: "username", DataType: "VARCHAR(50)", Options: "NOT NULL"},
		},
	}

	result := GenerateTriggers(table)

	expectedContains := []string{
		"CREATE OR REPLACE FUNCTION users_insert_history()",
		"CREATE OR REPLACE FUNCTION users_update_history()",
		"CREATE OR REPLACE FUNCTION users_delete_history()",
		"CREATE TRIGGER users_insert_trigger",
		"CREATE TRIGGER users_update_trigger",
		"CREATE TRIGGER users_delete_trigger",
		"INSERT INTO users_history",
		"UPDATE users_history SET valid_to = CURRENT_TIMESTAMP",
		"'I'",
		"'U'",
		"'D'",
	}

	for _, expected := range expectedContains {
		if !strings.Contains(result, expected) {
			t.Errorf("Expected result to contain '%s', but it didn't", expected)
		}
	}
}

func TestGetPrimaryKeyColumns(t *testing.T) {
	tests := []struct {
		name     string
		table    Table
		expected []string
	}{
		{
			name: "Single primary key",
			table: Table{
				Columns: []Column{
					{Name: "id", DataType: "SERIAL", Options: "PRIMARY KEY"},
					{Name: "name", DataType: "VARCHAR(50)", Options: "NOT NULL"},
				},
			},
			expected: []string{"id"},
		},
		{
			name: "No primary key - uses first column",
			table: Table{
				Columns: []Column{
					{Name: "name", DataType: "VARCHAR(50)", Options: "NOT NULL"},
					{Name: "email", DataType: "VARCHAR(100)", Options: ""},
				},
			},
			expected: []string{"name"},
		},
		{
			name: "Empty table",
			table: Table{
				Columns: []Column{},
			},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetPrimaryKeyColumns(tt.table)
			if len(result) != len(tt.expected) {
				t.Fatalf("Expected %d primary keys, got %d", len(tt.expected), len(result))
			}
			for i, expected := range tt.expected {
				if result[i] != expected {
					t.Errorf("Expected primary key %d to be '%s', got '%s'", i, expected, result[i])
				}
			}
		})
	}
}

func TestFileOperations(t *testing.T) {
	testFilename := "test_file.sql"
	testContent := "CREATE TABLE test (id INTEGER);"

	defer os.Remove(testFilename)

	err := writeFile(testFilename, testContent)
	if err != nil {
		t.Fatalf("Expected no error writing file, got: %v", err)
	}

	content, err := readFile(testFilename)
	if err != nil {
		t.Fatalf("Expected no error reading file, got: %v", err)
	}

	if content != testContent {
		t.Errorf("Expected content '%s', got: '%s'", testContent, content)
	}
}

func TestGenerateHistorySQL(t *testing.T) {
	tables := []Table{
		{
			Name: "users",
			Columns: []Column{
				{Name: "id", DataType: "SERIAL", Options: "PRIMARY KEY"},
				{Name: "username", DataType: "VARCHAR(50)", Options: "NOT NULL"},
			},
		},
	}

	result, err := GenerateHistorySQL(tables)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	expectedContains := []string{
		"-- Generated History Tables and Triggers",
		"CREATE TABLE users_history",
		"CREATE OR REPLACE FUNCTION users_insert_history()",
		"-- Usage Examples:",
	}

	for _, expected := range expectedContains {
		if !strings.Contains(result, expected) {
			t.Errorf("Expected result to contain '%s', but it didn't", expected)
		}
	}
}

func TestSchemaHelperFunctions(t *testing.T) {
	tests := []struct {
		name         string
		table        Table
		expectedHist string
		expectedOrig string
		expectedFunc string
	}{
		{
			name:         "Without schema",
			table:        Table{Name: "users", SchemaName: ""},
			expectedHist: "users_history",
			expectedOrig: "users",
			expectedFunc: "users",
		},
		{
			name:         "With schema",
			table:        Table{Name: "products", SchemaName: "inventory"},
			expectedHist: "inventory.products_history",
			expectedOrig: "inventory.products",
			expectedFunc: "inventory_products",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetHistoryTableName(tt.table); got != tt.expectedHist {
				t.Errorf("getHistoryTableName() = %v, want %v", got, tt.expectedHist)
			}
			if got := GetOriginalTableName(tt.table); got != tt.expectedOrig {
				t.Errorf("getOriginalTableName() = %v, want %v", got, tt.expectedOrig)
			}
			if got := GetFunctionPrefix(tt.table); got != tt.expectedFunc {
				t.Errorf("getFunctionPrefix() = %v, want %v", got, tt.expectedFunc)
			}
		})
	}
}

func readFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}

	return string(content), nil
}

func TestParseForeignKeys(t *testing.T) {
	sqlContent := `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(50) NOT NULL
		);
		
		CREATE TABLE orders (
			order_id SERIAL PRIMARY KEY,
			user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
			total DECIMAL(10,2) NOT NULL,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL ON UPDATE CASCADE
		);
	`

	tables, err := ParseCreateTables(sqlContent)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	ordersTable := tables[1]
	if len(ordersTable.ForeignKeys) != 2 {
		t.Fatalf("Expected 2 foreign keys in orders table, got: %d", len(ordersTable.ForeignKeys))
	}

	inlineFk := ordersTable.ForeignKeys[0]
	if inlineFk.ColumnName != "user_id" {
		t.Errorf("Expected inline FK column 'user_id', got: %s", inlineFk.ColumnName)
	}
	if inlineFk.ReferencedTable != "users" {
		t.Errorf("Expected inline FK table 'users', got: %s", inlineFk.ReferencedTable)
	}
	if inlineFk.ReferencedColumn != "id" {
		t.Errorf("Expected inline FK column 'id', got: %s", inlineFk.ReferencedColumn)
	}
	if inlineFk.OnDelete != "CASCADE" {
		t.Errorf("Expected inline FK ON DELETE 'CASCADE', got: %s", inlineFk.OnDelete)
	}

	constraintFk := ordersTable.ForeignKeys[1]
	if constraintFk.ColumnName != "user_id" {
		t.Errorf("Expected constraint FK column 'user_id', got: %s", constraintFk.ColumnName)
	}
	if constraintFk.OnDelete != "SET NULL" {
		t.Errorf("Expected constraint FK ON DELETE 'SET NULL', got: %s", constraintFk.OnDelete)
	}
	if constraintFk.OnUpdate != "CASCADE" {
		t.Errorf("Expected constraint FK ON UPDATE 'CASCADE', got: %s", constraintFk.OnUpdate)
	}
}

func writeFile(filename, content string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(content)
	return err
}

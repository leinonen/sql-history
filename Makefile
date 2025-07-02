.PHONY: build test test-unit test-integration docker-up docker-down docker-logs clean

# Build the application
build:
	go build -o bin/sql-history cmd/sql-history/main.go

# Install dependencies
deps:
	go mod tidy

# Run all tests
test: test-unit test-integration

# Run unit tests only
test-unit:
	go test -v -short ./pkg/...

# Run integration tests (requires Docker)
test-integration: docker-up
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 10
	go test -v ./test/...
	@$(MAKE) docker-down

# Start PostgreSQL with Docker Compose
docker-up:
	docker compose up -d
	@echo "PostgreSQL starting up..."

# Stop PostgreSQL
docker-down:
	docker compose down

# View PostgreSQL logs
docker-logs:
	docker compose logs -f postgres

# Clean up build artifacts and Docker volumes
clean:
	rm -rf bin/
	docker compose down -v
	docker system prune -f

# Run a complete test cycle
ci: clean deps build test-unit test-integration
	@echo "All tests passed!"

# Connect to the test database (for debugging)
db-connect:
	docker exec -it sql-history-postgres psql -U testuser -d testdb

# Example: Generate history tables from example file
example: build
	./bin/sql-history sql/example.sql sql/example_history.sql
	@echo "Generated sql/example_history.sql"

# Example: Generate history tables from schema example
example-schema: build
	./bin/sql-history sql/example_with_schema.sql sql/example_with_schema_history.sql
	@echo "Generated sql/example_with_schema_history.sql"

# Lint the code
lint:
	golangci-lint run

# Format the code
fmt:
	go fmt ./...
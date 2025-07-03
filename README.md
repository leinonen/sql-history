# SQL History Table Generator

Generates history tables with temporal tracking for PostgreSQL from CREATE TABLE statements.

## Features

- **History Tables**: Automatic generation with `valid_from`, `valid_to`, `operation` columns
- **User Tracking**: Optional `changed_by` column to track who made changes
- **Triggers**: INSERT/UPDATE/DELETE triggers for automatic tracking
- **Foreign Keys**: Parses and preserves relationships (inline and explicit syntax)
- **Schemas**: Supports schema-qualified table names
- **Point-in-Time Queries**: Generated examples for temporal data access

## Quick Start

```bash
make build
./bin/sql-history schema.sql
```

Input SQL with foreign keys:
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    total DECIMAL(10,2) NOT NULL
);
```

Generates `schema_history.sql` with:
- History tables (`users_history`, `orders_history`) 
- Triggers for automatic tracking
- Example temporal queries

## How It Works

### History Tables
Each table gets a corresponding `{table}_history` table with:
- All original columns (without PRIMARY KEY constraints)
- `valid_from TIMESTAMP` - When record became active
- `valid_to TIMESTAMP` - When superseded (NULL = current)
- `operation CHAR(1)` - 'I' (Insert), 'U' (Update), 'D' (Delete)
- `changed_by VARCHAR(255)` - Who made the change (optional, with `--track-user`)

### Triggers
- **INSERT**: Records new data with `operation = 'I'`
- **UPDATE**: Closes previous record, inserts new with `operation = 'U'`  
- **DELETE**: Marks record deleted with `operation = 'D'`

### Point-in-Time Queries
```sql
-- View table at specific time
SELECT * FROM users_history 
WHERE valid_from <= '2024-01-01 12:00:00' 
  AND (valid_to IS NULL OR valid_to > '2024-01-01 12:00:00')
  AND operation != 'D';

-- View current active records
SELECT * FROM users_history 
WHERE valid_to IS NULL 
  AND operation != 'D';
```

## Foreign Key Support

Supports both inline and explicit foreign key syntax:

```sql
-- Inline
user_id INTEGER REFERENCES users(id) ON DELETE CASCADE

-- Explicit  
FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL ON UPDATE CASCADE
```

**Note**: PostgreSQL cascading deletes don't fire user triggers, so cascaded child deletes won't appear in history.

## Development

```bash
make build              # Build binary
make test               # Run all tests
make test-integration   # Integration tests (requires Docker)
make docker-up          # Start PostgreSQL for testing
make build-all          # Build cross-platform binaries
make release            # Prepare release (test + build-all)
```

## Release Management

This project uses automated releases via GitHub Actions. Here's how to manage releases:

### Creating a Release

1. **Prepare the release**:
   ```bash
   make release  # Runs tests and builds all platforms
   ```

2. **Update version and create tag**:
   ```bash
   make release-tag  # Interactive - prompts for version number
   ```

3. **Automated process**: Once the tag is pushed, GitHub Actions will:
   - Run full test suite
   - Build cross-platform binaries (Linux, macOS, Windows)
   - Create GitHub release with binaries
   - Generate release notes

### Release Process

- **Semantic Versioning**: Use `v1.2.3` format
- **Automated Builds**: GitHub Actions builds for Linux, macOS, and Windows
- **Release Notes**: Auto-generated from tag and commit history
- **Binaries**: Available for download from GitHub releases

### Manual Release Steps

If you need to create a release manually:

1. Update version in `cmd/sql-history/main.go`
2. Update `CHANGELOG.md` with new version
3. Commit changes: `git commit -m "Bump version to v1.x.x"`
4. Create and push tag: `git tag -a v1.x.x -m "Release v1.x.x" && git push origin v1.x.x`
5. GitHub Actions will handle the rest

## Usage

```bash
./bin/sql-history [flags] input.sql [output.sql]

# Examples
./bin/sql-history schema.sql                    # → schema_history.sql
./bin/sql-history tables.sql history_tables.sql # → history_tables.sql

# With user tracking
./bin/sql-history --track-user schema.sql       # → schema_history.sql (with changed_by column)
./bin/sql-history --track-user --user-source session schema.sql # → uses session variable
```

### Flags

- `--track-user`: Add `changed_by` column to history tables for user tracking
- `--user-source`: Source for user information (default: `current_user`)
  - `current_user`: Uses PostgreSQL's built-in `current_user` function
  - `session`: Uses `current_setting('app.current_user', true)` with fallback to `current_user`

### User Tracking

When `--track-user` is enabled, history tables include a `changed_by` column:

```sql
-- With --user-source current_user (default)
INSERT INTO users_history (..., changed_by) VALUES (..., current_user);

-- With --user-source session
INSERT INTO users_history (..., changed_by) VALUES (..., COALESCE(current_setting('app.current_user', true), current_user));
```

For session-based tracking, set the user in your application:
```sql
-- Set current user for the session
SELECT set_config('app.current_user', 'john.doe', false);
```

## Limitations

- PostgreSQL only (uses PL/pgSQL)
- Basic SQL parsing (common CREATE TABLE patterns)
- Cascading deletes don't trigger history recording (PostgreSQL behavior)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
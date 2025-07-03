# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.2] - 2025-07-03

### Added
- Initial release of SQL History Table Generator
- CREATE TABLE parsing for PostgreSQL
- History table generation with temporal tracking (`valid_from`, `valid_to`, `operation`)
- Automatic trigger generation for INSERT/UPDATE/DELETE operations
- Foreign key support (both inline and explicit syntax)
- Schema-qualified table name support
- User tracking with `--track-user` flag and `changed_by` column
- Configurable user source (`--user-source` flag) for session-based or current_user tracking
- Point-in-time query examples in generated output
- Version flag (`--version`) to display application version
- Cross-platform binary generation (Linux, macOS, Windows)
- Comprehensive test suite with unit and integration tests
- GitHub Actions CI/CD pipeline for automated testing and releases
- Docker Compose setup for local PostgreSQL testing
- Makefile with build, test, and development targets

### Features
- **History Tables**: Automatic generation with temporal tracking columns
- **User Tracking**: Optional change tracking with configurable user sources
- **Triggers**: Automatic INSERT/UPDATE/DELETE triggers for history recording
- **Foreign Keys**: Preserves relationships in generated history tables
- **Schemas**: Full support for schema-qualified table names
- **Point-in-Time Queries**: Generated examples for temporal data access
- **CLI Interface**: Simple command-line interface with helpful flags

### Technical Details
- Written in Go 1.21
- Uses PostgreSQL-specific features (PL/pgSQL triggers)
- Supports both inline and explicit foreign key syntax
- Handles cascading deletes appropriately
- Includes comprehensive error handling and validation

[1.0.0]: https://github.com/leinonen/sql-history/releases/tag/v1.0.0
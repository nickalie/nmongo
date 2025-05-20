# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

nmongo is a Go-based CLI tool for MongoDB operations. The main functionality is copying data between MongoDB clusters with features like incremental copying, collection index copying, and selective database/collection copying.

## Commands

### Build and Install

```bash
# Build the application
make build

# Install to GOPATH
make install

# Run the application
make run

# Clean build artifacts
make clean
```

### Testing

```bash
# Run all tests (requires Docker)
make test
# or
go test -v ./...

# Run specific test
go test -v ./internal/mongodb -run TestClient

# Run with verbose output
go test -v ./internal/mongodb
```

### Linting

```bash
# Run the linter
make lint
# or
golangci-lint run ./...
```

### Generate Documentation

```bash
# Generate and serve documentation
make doc
```

## Code Architecture

### Core Components

1. **Command Line Interface (cmd/)**
   - Uses Cobra library for CLI functionality
   - Main commands: `copy` and `config`
   - Handles flag parsing, configuration loading/saving, and command execution

2. **MongoDB Operations (internal/mongodb/)**
   - `Client`: Wrapper around the MongoDB Go driver client
   - Collection copying functionality with support for indexes and incremental copying
   - Handles batch operations for efficient data transfer

3. **Configuration Management (internal/config/)**
   - Configuration loading/saving in multiple formats (JSON, YAML, TOML)
   - Default configuration handling
   - Command-specific configuration options

### Key Functions and Files

- `cmd/copy.go`: Implements the copy command for transferring data between MongoDB clusters
- `cmd/config.go`: Manages configuration operations for saving/loading settings
- `internal/mongodb/client.go`: MongoDB client wrapper with utility functions
- `internal/mongodb/incremental.go`: Handles incremental copying logic with last-modified tracking
- `internal/config/config.go`: Configuration structure and persistence

## Important Notes

1. The application uses testcontainers for integration testing, which requires Docker to be running when tests are executed.

2. Incremental copying relies on a timestamp field in the documents to track changes. By default, this is `lastModified` but can be configured.

3. MongoDB connection supports TLS with optional CA certificate configuration.

4. The application stores configuration files in the user's home directory under `.nmongo/` with support for multiple formats.

5. Run "golangci-lint run ./..." to make sure all code updates are linted. Always address linting issues.

6. Run "go test ./..." to make sure all code updates are tested.

7. Make sure test coverage is above 80%.
8. Don't run gofmt for particular file to fix formating issues. Always run "go fmt ./..." to make sure all code updates are formatted.
9. Update readme when adding new features.

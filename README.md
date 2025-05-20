# nmongo

A Go-based CLI tool for MongoDB operations.

## Overview

`nmongo` is a command-line interface tool written in Go that provides various MongoDB operations. Currently, it supports copying data between MongoDB clusters with the following features:

- Copy all databases and collections from one MongoDB cluster to another
- Support for incremental copying to only transfer new or updated documents
- Filtering by specific databases or collections
- Adjustable batch size for optimized performance
- Save and load configurations from files

## Installation

### Using Go

```bash
go install github.com/yourusername/nmongo@latest
```

### From Source

```bash
git clone https://github.com/yourusername/nmongo.git
cd nmongo
go build
```

## Usage

### Copy Command

Copy data between MongoDB clusters:

```bash
nmongo copy --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017"
```

#### Options

- `--source`: Source MongoDB connection string (required)
- `--target`: Target MongoDB connection string (required)
- `--incremental`: Perform incremental copy (only copy new or updated documents)
- `--timeout`: Connection timeout in seconds (default: 30)
- `--databases`: List of specific databases to copy (default: all non-system databases)
- `--collections`: List of specific collections to copy (default: all non-system collections)
- `--batch-size`: Batch size for document operations (default: 1000)
- `--config`: Path to configuration file
- `--save-config`: Save current flags to configuration file

### Configuration Management

Save current settings to configuration file:

```bash
nmongo config save
```

Show current configuration:

```bash
nmongo config show
```

## Examples

Copy all databases:
```bash
nmongo copy --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017"
```

Copy specific databases:
```bash
nmongo copy --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --databases="db1,db2"
```

Incremental copy:
```bash
nmongo copy --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --incremental
```

Save configuration for future use:
```bash
nmongo copy --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --save-config
```

Use saved configuration:
```bash
nmongo copy
```

## Development

### Testing

Tests are implemented using:
- **TestContainers**: Automatically creates and manages MongoDB containers for testing
- **Testify**: For improved test assertions and readability

To run the tests:

```bash
go test -v ./tests
```

Note: Running tests requires Docker to be installed and running on your machine.

The test suite includes:
- Basic client functionality tests
- Incremental copy tests
- Full database copy tests
- Document operation tests (using table-driven tests)

### Building

```bash
make build
```

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

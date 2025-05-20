# nmongo

A Go-based CLI tool for MongoDB operations.

## Overview

`nmongo` is a command-line interface tool written in Go that provides various MongoDB operations. Currently, it supports copying data between MongoDB clusters with the following features:

- Copy all databases and collections from one MongoDB cluster to another
- Copy collection indexes along with the data to preserve query performance
- Support for incremental copying to only transfer new or updated documents
- Customizable field for tracking last modified documents in incremental copies
- Include or exclude specific databases and collections
- Adjustable batch size for optimized performance
- Save and load configurations from files in multiple formats (JSON, YAML, TOML)
- Support for secure connections with custom CA certificates

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
- `--source-ca-cert-file`: Path to CA certificate file for source MongoDB TLS connections
- `--target-ca-cert-file`: Path to CA certificate file for target MongoDB TLS connections
- `--incremental`: Perform incremental copy (only copy new or updated documents)
- `--timeout`: Connection timeout in seconds (default: 120)
- `--databases`: List of specific databases to copy (default: all non-system databases)
- `--collections`: List of specific collections to copy (default: all non-system collections)
- `--exclude-databases`: List of databases to exclude from copy
- `--exclude-collections`: List of collections to exclude from copy
- `--batch-size`: Batch size for document operations (default: 1000)
- `--last-modified-field`: Field name to use for tracking document modifications in incremental copy (default: "lastModified")
- `--config`: Path to configuration file
- `--save-config`: Save current flags to configuration file
- `--config-format`: Configuration file format for saving (json, yaml, or toml)

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

Exclude specific databases:
```bash
nmongo copy --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --exclude-databases="admin,local,config"
```

Incremental copy:
```bash
nmongo copy --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --incremental
```

Incremental copy with custom last modified field:
```bash
nmongo copy --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --incremental --last-modified-field="updatedAt"
```

Save configuration for future use:
```bash
nmongo copy --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --save-config
```

Use saved configuration:
```bash
nmongo copy
```

## MongoDB Client Configuration

### TLS Support

The MongoDB client supports TLS connections with custom CA certificates.

### Using CA Certificate Files

You can specify separate CA certificates for source and target MongoDB connections:

```bash
nmongo copy --source "mongodb://user:password@your-mongodb-host:27018/" \
            --target "mongodb://localhost:27017" \
            --source-ca-cert-file /path/to/source-certificate.crt \
            --target-ca-cert-file /path/to/target-certificate.crt
```


#### Windows Path Support

On Windows, you can use both forward slashes or backslashes in the certificate path:

```bash
nmongo copy --source "mongodb://user:password@your-mongodb-host:27018/" \
            --target "mongodb://localhost:27017" \
            --source-ca-cert-file "C:\path\to\source-certificate.crt" \
            --target-ca-cert-file "C:\path\to\target-certificate.crt"
```

### Managed Cloud MongoDB Services

When connecting to managed MongoDB services like MongoDB Atlas, Azure Cosmos DB, or other cloud providers, you'll need to:

1. Download the CA certificate from your cloud provider (if required)
2. Specify the certificate file with the `--ca-cert-file` flag
3. Use the MongoDB connection string provided by your cloud service

Example:

```bash
nmongo copy --source "mongodb://user:password@your-cluster.mongodb.net:27018/" \
            --target "mongodb://localhost:27017" \
            --source-ca-cert-file cloud-provider-ca.crt \
            --incremental \
            --last-modified-field createdAt
```

### Connection Timeout 

For slow connections or when connecting to remote cloud databases, you may need to increase the connection timeout:

```bash
nmongo copy --source "mongodb://user:password@your-mongodb-host:27018/" \
            --target "mongodb://localhost:27017" \
            --source-ca-cert-file /path/to/source-certificate.crt \
            --target-ca-cert-file /path/to/target-certificate.crt \
            --timeout 120
```

The default timeout is now 120 seconds, which should be sufficient for most cloud connections.

### Programmatic Usage

When using the client programmatically:

```go
client, err := mongodb.NewClient(ctx, "mongodb://user:password@host:port/", "/path/to/ca-cert.pem")
if err != nil {
    log.Fatalf("Failed to create MongoDB client: %v", err)
}
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

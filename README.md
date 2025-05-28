# nmongo

A Go-based CLI tool for MongoDB operations.

## Overview

`nmongo` is a command-line interface tool written in Go that provides various MongoDB operations. It supports:

- Copying data between MongoDB clusters with all indexes
- Comparing data between MongoDB clusters to identify differences
- Creating incremental database dumps using mongodump
- Restoring databases from dumps using mongorestore
- Incremental copying to only transfer new or updated documents
- Include or exclude specific databases and collections
- Adjustable batch size for optimized performance
- Automatic retry with exponential backoff for transient failures
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
- `--retry-attempts`: Number of retry attempts for failed operations (default: 5)
- `--config`: Path to configuration file
- `--save-config`: Save current flags to configuration file
- `--config-format`: Configuration file format for saving (json, yaml, or toml)

### Dump Command

Create incremental dumps of MongoDB databases using the mongodump CLI tool:

```bash
nmongo dump --source "mongodb://source-host:27017" --output ./dumps
```

#### Options

- `--source`: Source MongoDB connection string (required)
- `--source-ca-cert-file`: Path to CA certificate file for source MongoDB TLS connections
- `--output`: Output directory for dump files (default: "./dumps")
- `--incremental`: Perform incremental dump (only dump new or updated documents)
- `--timeout`: Connection timeout in seconds (default: 30)
- `--databases`: List of specific databases to dump (default: all non-system databases)
- `--collections`: List of specific collections to dump (default: all non-system collections)
- `--exclude-databases`: List of databases to exclude from dump
- `--exclude-collections`: List of collections to exclude from dump
- `--last-modified-field`: Field name to use for tracking document modifications in incremental dump (default: "lastModified")
- `--retry-attempts`: Number of retry attempts for failed operations (default: 5)
- `--state-file`: Path to state file for tracking dump progress (defaults to `<output>/dump-state.json`)
- `--config`: Path to configuration file
- `--save-config`: Save current flags to configuration file

**Note**: The dump command requires the `mongodump` CLI tool to be installed and available in your PATH.

### Restore Command

Restore MongoDB databases from dumps created by the dump command using the mongorestore CLI tool:

```bash
nmongo restore --target "mongodb://target-host:27017" --input ./dumps
```

#### Options

- `--target`: Target MongoDB connection string (required)
- `--target-ca-cert-file`: Path to CA certificate file for target MongoDB TLS connections
- `--input`: Input directory containing dump files (default: "./dumps")
- `--timeout`: Connection timeout in seconds (default: 30)
- `--databases`: List of specific databases to restore (default: all found in dumps)
- `--collections`: List of specific collections to restore (default: all found in dumps)
- `--exclude-databases`: List of databases to exclude from restore
- `--exclude-collections`: List of collections to exclude from restore
- `--retry-attempts`: Number of retry attempts for failed operations (default: 5)
- `--state-file`: Path to state file for tracking restore progress (defaults to `<input>/restore-state.json`)
- `--drop`: Drop collections before restoring (default: false)
- `--oplog-replay`: Replay oplog after restoring (default: false)
- `--preserve-dates`: Preserve original document timestamps (default: true)
- `--config`: Path to configuration file
- `--save-config`: Save current flags to configuration file

**Note**: The restore command requires the `mongorestore` CLI tool to be installed and available in your PATH.

### Compare Command

Compare data between MongoDB clusters to identify differences:

```bash
nmongo compare --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017"
```

#### Options

- `--source`: Source MongoDB connection string (required)
- `--target`: Target MongoDB connection string (required)
- `--source-ca-cert-file`: Path to CA certificate file for source MongoDB TLS connections
- `--target-ca-cert-file`: Path to CA certificate file for target MongoDB TLS connections
- `--timeout`: Connection timeout in seconds (default: 30)
- `--databases`: List of specific databases to compare (default: all non-system databases)
- `--collections`: List of specific collections to compare (default: all non-system collections)
- `--exclude-databases`: List of databases to exclude from comparison
- `--exclude-collections`: List of collections to exclude from comparison
- `--batch-size`: Batch size for document operations (default: 10000)
- `--detailed`: Perform detailed document-by-document comparison (slower but more comprehensive)
- `--output`: Write comparison results to specified JSON file
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

### Copy Examples

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

Incremental copy when source database user doesn't have permissions for metadata:
```bash
nmongo copy --source "mongodb://readonly-user:pwd@source-host:27017" --target "mongodb://readwrite-user:pwd@dest-host:27017" --incremental
```

Note: The incremental copy feature now automatically detects if the source database user doesn't have sufficient permissions and will store the metadata in the target database instead. By default, the metadata for tracking incremental copies is stored in the target database, which is usually preferable as:

1. Source databases often have read-only permissions
2. Target databases typically have write permissions required for metadata
3. This maintains a single source of truth for synchronization state

In rare cases where the target database doesn't have appropriate permissions, the system will automatically attempt to use the source database for metadata storage.

Save configuration for future use:
```bash
nmongo copy --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --save-config
```

Use saved configuration:
```bash
nmongo copy
```

Copy with custom retry attempts:
```bash
nmongo copy --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --retry-attempts=10
```

### Compare Examples

Basic comparison (document counts only):
```bash
nmongo compare --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017"
```

Detailed document-by-document comparison:
```bash
nmongo compare --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --detailed
```

Compare specific databases:
```bash
nmongo compare --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --databases="db1,db2"
```

Compare specific collections:
```bash
nmongo compare --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --collections="users,products"
```

Save comparison results to a file:
```bash
nmongo compare --source "mongodb://source-host:27017" --target "mongodb://dest-host:27017" --output="comparison.json"
```

### Dump Examples

Basic dump of all databases:
```bash
nmongo dump --source "mongodb://source-host:27017" --output ./dumps
```

Dump specific databases:
```bash
nmongo dump --source "mongodb://source-host:27017" --output ./dumps --databases="db1,db2"
```

Incremental dump (only new/updated documents since last dump):
```bash
nmongo dump --source "mongodb://source-host:27017" --output ./dumps --incremental
```

Incremental dump with custom timestamp field:
```bash
nmongo dump --source "mongodb://source-host:27017" --output ./dumps --incremental --last-modified-field="updatedAt"
```

Dump with custom state file location:
```bash
nmongo dump --source "mongodb://source-host:27017" --output ./dumps --incremental --state-file="/var/lib/nmongo/dump-state.json"
```

Exclude system databases from dump:
```bash
nmongo dump --source "mongodb://source-host:27017" --output ./dumps --exclude-databases="admin,local,config"
```

### Restore Examples

Basic restore of all databases from dumps:
```bash
nmongo restore --target "mongodb://target-host:27017" --input ./dumps
```

Restore specific databases:
```bash
nmongo restore --target "mongodb://target-host:27017" --input ./dumps --databases="db1,db2"
```

Restore with drop collections first (clean restore):
```bash
nmongo restore --target "mongodb://target-host:27017" --input ./dumps --drop
```

Restore specific collections:
```bash
nmongo restore --target "mongodb://target-host:27017" --input ./dumps --collections="users,products"
```

Restore with custom state file location:
```bash
nmongo restore --target "mongodb://target-host:27017" --input ./dumps --state-file="/var/lib/nmongo/restore-state.json"
```

Exclude certain databases from restore:
```bash
nmongo restore --target "mongodb://target-host:27017" --input ./dumps --exclude-databases="test,staging"
```

Restore with oplog replay (for point-in-time recovery):
```bash
nmongo restore --target "mongodb://target-host:27017" --input ./dumps --oplog-replay
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

### Connection and Operation Timeouts

The application has an enhanced timeout system:

1. **Connection Timeout**: Controlled by the `--timeout` flag and only applies to the initial connection
2. **Operation Timeouts**: Longer timeouts are automatically used for data operations:
   - 30 minutes for data cursors 
   - 5 minutes for index operations
   - 5 minutes for socket timeouts

Example of setting the connection timeout:

```bash
nmongo copy --source "mongodb://user:password@your-mongodb-host:27018/" \
            --target "mongodb://localhost:27017" \
            --timeout 60
```

For large databases that were failing with timeout errors, you should no longer need to increase the timeout value since data operations now use separate, longer timeouts automatically.

#### Progress Updates

The application now displays regular progress updates every 10 seconds during data copying operations, helping you monitor long-running operations.

#### Retry Mechanism

The copy command includes automatic retry logic for handling transient failures:

- **Default retry attempts**: 5 (configurable with `--retry-attempts`)
- **Exponential backoff**: Starting at 100ms with up to 30 seconds maximum
- **Retryable errors**: Network timeouts, connection failures, primary stepdowns, write conflicts
- **Non-retryable errors**: Duplicate key errors, authentication failures, invalid operations

The retry mechanism helps ensure reliable data copying even in the presence of temporary network issues or MongoDB cluster failovers.

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

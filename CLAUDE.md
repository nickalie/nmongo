1. The application uses testcontainers for integration testing, which requires Docker to be running when tests are executed.
2. Incremental copying relies on a timestamp field in the documents to track changes. By default, this is `lastModified` but can be configured.
3. MongoDB connection supports TLS with optional CA certificate configuration.
4. Run "golangci-lint run ./..." to make sure all code updates are linted. Always address linting issues.
5. Run "go test ./..." to make sure all code updates are tested.
6. Make sure test coverage is above 80%.
7. Don't run gofmt for particular file to fix formating issues. Always run "go fmt ./..." to make sure all code updates are formatted.
8. Update readme when adding new features.

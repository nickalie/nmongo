1. The application uses testcontainers for integration testing, which requires Docker to be running when tests are executed.
2. Incremental copying relies on a timestamp field in the documents to track changes. By default, this is `lastModified` but can be configured.
3. MongoDB connection supports TLS with optional CA certificate configuration.
4. Always add tests for any new features
5. Always run "golangci-lint run ./..." when you’re done making a series of code changes
6. Always run "go test ./... -cover" when you’re done making a series of code changes. Make sure test coverage is above 80%. If not add missing tests
7. Always run "go fmt ./..." when you’re done making a series of code changes
8. Update readme when adding new features.

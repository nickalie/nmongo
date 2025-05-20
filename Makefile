.PHONY: build test clean

# Default build target
build:
	go build -o nmongo

# Run tests
test:
	go test -v ./...

# Install to GOPATH
install:
	go install

# Clean build artifacts
clean:
	rm -f nmongo
	rm -f nmongo.exe

# Run with simple args (for testing)
run:
	go run main.go

# Check code quality
lint:
	golangci-lint run ./...

# Generate documentation
doc:
	@command -v godoc >/dev/null 2>&1 || { echo >&2 "godoc not installed. Running: go install golang.org/x/tools/cmd/godoc@latest"; go install golang.org/x/tools/cmd/godoc@latest; }
	@echo "Starting godoc server on http://localhost:6060"
	@echo "View documentation at http://localhost:6060/pkg/nmongo/"
	godoc -http=:6060

# Version info
version:
	@echo "nmongo version 0.1.0"

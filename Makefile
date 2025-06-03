.PHONY: build test clean

# Version information
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
DATE := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS := -ldflags "-s -w -X nmongo/internal/version.Version=$(VERSION) -X nmongo/internal/version.Commit=$(COMMIT) -X nmongo/internal/version.Date=$(DATE)"

# Default build target
build:
	go build $(LDFLAGS) -o nmongo

# Run tests
test:
	go test -v ./...

# Install to GOPATH
install:
	go install $(LDFLAGS)

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


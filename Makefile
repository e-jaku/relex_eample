PKG := ./...
COVERAGE_FILE := coverage.out

.PHONY: all test test-coverage build run clean

all: docker-up

test:
	@echo "Running tests..."
	go test $(PKG)

test-coverage:
	@echo "Running tests with coverage..."
	go test -coverprofile=$(COVERAGE_FILE) $(PKG)	

build:
	@echo "Running build..."
	go build -o ./bin/backend ./cmd/backend/main.go

run: build
	@echo "Running application..."
	./bin/backend

clean:
	@echo "Running clean..."
	@rm -rf bin
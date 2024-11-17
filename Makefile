PKG := ./...
COVERAGE_FILE := coverage.out

.PHONY: all test test-coverage test-with-backend build run clean

all: docker-up

test:
	@echo "Running tests..."
	go test $(PKG) -count=1

test-with-backend: run-for-test
	@echo "Running test with running backend..."
# enforce running stop independent if test fails or succeed
	@trap "$(MAKE) stop" EXIT; \
	go test $(PKG) -count=1

test-coverage:
	@echo "Running tests with coverage..."
	go test -coverprofile=$(COVERAGE_FILE) $(PKG)	

build:
	@echo "Running build..."
	go build -o ./bin/backend ./cmd/backend/main.go

run: build
	@echo "Running application..."
	./bin/backend

run-for-test: build
	@echo "Running application for integration testing..."
	./bin/backend & echo $$! > server.pid

stop: clean
	@echo "Stop application running in background after testing is done..."
	@kill `cat server.pid`
	@rm server.pid
	
clean:
	@echo "Running clean..."
	@rm -rf bin
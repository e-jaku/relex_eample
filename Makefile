PKG := ./...
COVERAGE_FILE := coverage.out

.PHONY: all test test-coverage build run clean

all: docker-up

test: run-test
	@echo "Running tests..."
# enforce running stop independednt if test fail or succeede	
	@trap "$(MAKE) stop" EXIT; \
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

run-test: build
	@echo "Running application for integration testing..."
	./bin/backend & echo $$! > server.pid

stop: clean
	@echo "Stop application running in background after testing is done..."
	@kill `cat server.pid`
	@rm server.pid
	
clean:
	@echo "Running clean..."
	@rm -rf bin
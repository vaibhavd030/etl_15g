# O2 ETL Pipeline Makefile

.PHONY: help install run test test-unit test-integration test-coverage analyze clean docker-build docker-run docker-clean logs all

# Default target
help:
	@echo "O2 ETL Pipeline - Available Commands:"
	@echo "======================================"
	@echo "  make install         - Install Python dependencies"
	@echo "  make run            - Run the ETL pipeline locally"
	@echo "  make analyze        - Analyze JSON structure"
	@echo "  make test           - Run all tests"
	@echo "  make test-unit      - Run unit tests only"
	@echo "  make test-integration - Run integration tests"
	@echo "  make test-coverage  - Run tests with coverage report"
	@echo "  make clean          - Clean output and log files"
	@echo "  make logs           - View recent logs"
	@echo "  make docker-build   - Build Docker image"
	@echo "  make docker-run     - Run pipeline in Docker"
	@echo "  make docker-clean   - Remove Docker containers/images"
	@echo "  make all            - Install, test, and run"

# Install dependencies
install:
	@echo "Installing dependencies..."
	@pip install -r requirements.txt
	@echo "✓ Dependencies installed"

# Run the pipeline
run:
	@echo "Running ETL Pipeline..."
	@mkdir -p output logs
	@python -m src.pipeline

# Analyze JSON structure
analyze:
	@echo "Analyzing JSON structure..."
	@python -m src.analyze_json data_input/o2-product-set.json

# Run all tests
test:
	@echo "Running all tests..."
	@pytest tests/ -v

# Run unit tests only
test-unit:
	@echo "Running unit tests..."
	@pytest tests/test_models.py tests/test_pipeline.py -v

# Run integration tests
test-integration:
	@echo "Running integration tests..."
	@pytest tests/test_integration.py -v

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@pytest --cov=. --cov-report=term-missing --cov-report=html tests/
	@echo "Coverage report generated in htmlcov/index.html"

# Clean outputs
clean:
	@echo "Cleaning output files..."
	@rm -rf output/*.json output/*.csv
	@rm -rf logs/*.log
	@rm -rf __pycache__ *.pyc
	@rm -rf .pytest_cache
	@echo "✓ Cleaned"

# View logs
logs:
	@echo "Recent pipeline logs:"
	@echo "====================="
	@tail -n 50 logs/etl_pipeline.log 2>/dev/null || echo "No logs found. Run 'make run' first."

# Docker commands
docker-build:
	@command -v docker >/dev/null 2>&1 || { echo "Docker not installed. Skipping..."; exit 0; }
	@echo "Building Docker image..."
	@docker build -t o2-etl:latest .
	@echo "✓ Docker image built"

docker-run:
	@command -v docker >/dev/null 2>&1 || { echo "Docker not installed. Skipping..."; exit 0; }
	@echo "Running pipeline in Docker..."
	@docker run -v $(PWD):/data o2-etl:latest

docker-clean:
	@echo "Cleaning Docker resources..."
	@docker stop o2-etl 2>/dev/null || true
	@docker rm o2-etl 2>/dev/null || true
	@docker rmi o2-etl:latest 2>/dev/null || true
	@echo "✓ Docker resources cleaned"

# Run everything
all: install test run
	@echo "✓ Pipeline execution complete!"
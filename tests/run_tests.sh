#!/bin/bash

# O2 ETL Pipeline - Test Runner

echo "======================================"
echo "O2 ETL Pipeline - Test Suite"
echo "======================================"

# Install test dependencies if needed
pip install -q pytest pytest-cov

echo ""
echo "Running Unit Tests..."
echo "----------------------"
pytest tests/test_models.py tests/test_pipeline.py -v --tb=short

echo ""
echo "Running Integration Tests..."
echo "-----------------------------"
pytest tests/test_integration.py -v --tb=short

echo ""
echo "Generating Coverage Report..."
echo "------------------------------"
pytest --cov=. --cov-report=term-missing --cov-report=html tests/

echo ""
echo "======================================"
echo "Test Summary"
echo "======================================"
echo "Coverage report available in htmlcov/index.html"
echo ""

# Show coverage summary
pytest --cov=. --cov-report=term tests/ --quiet
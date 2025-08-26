# O2 Product Catalogue ETL Pipeline

A production-ready ETL pipeline that transforms O2's product catalogue into clean, structured datasets for analytics and business intelligence.

## Business Problem

O2's product catalogue contains thousands of mixed products - phones, tariffs, accessories, insurance plans. The business needs **clean, validated data specifically for handsets and pay monthly tariffs** to enable:
- Analysis and optimization
- Tracking and forecasting
- Product recommendations

This pipeline solves this by intelligently filtering and transforming the raw catalogue without maintaining hardcoded product lists.

## Solution Architecture

### Data Flow Pipeline

```
                        ETL PIPELINE ARCHITECTURE
    
    ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
    │   EXTRACT    │      │  TRANSFORM   │      │     LOAD     │
    ├──────────────┤      ├──────────────┤      ├──────────────┤
    │              │      │              │      │              │
    │ • Read JSON  │──────► • Filter     │──────► • JSON       │
    │ • Parse Data │      │ • Validate   │      │ • CSV        │
    │ • Batch Load │      │ • Enrich     │      │ • Reports    │
    │              │      │ • Categorize │      │              │
    └──────────────┘      └──────────────┘      └──────────────┘
           │                     │                      │
           ▼                     ▼                      ▼
    ┌──────────────┐      ┌──────────────┐      ┌─────────────-─┐
    │   Raw Data   │      │   Pydantic   │      │    Outputs    │
    │              │      │   Models     │      │               │
    │ • Products   │      │ • Product    │      │ • Validated   │
    │ • All Types  │      │ • Transformed│      │ • Structured  │
    │ • Nested JSON│      │   Product    │      │ • Multi-format│
    └──────────────┘      └──────────────┘      └─────────────-─┘
```

### Processing Strategy

The pipeline implements **dynamic category detection**:

1. **Signal-based Classification**: Identifies products through multiple indicators
   - Presence of device specifications → Handset
   - Device options (colors, storage) → Handset
   - Brand/model patterns → Category inference
   - Keyword exclusions → Filter out insurance, accessories

2. **Data Enrichment**: Extracts and structures nested information
   - Storage options (128GB, 256GB, 1TB)
   - Color variants
   - Network technology (4G, 5G)
   - Device condition (New, Like New)

3. **Validation & Quality**: Ensures data integrity
   - Required fields validation
   - Type checking and coercion
   - Business rule enforcement (rating ranges, SKU generation)
   - Comprehensive error tracking

## Project Structure

```
o2-etl-pipeline/
│
├── src/                          # Source code
│   ├── __init__.py              # Package initializer
│   ├── pipeline.py              # Main ETL orchestrator
│   ├── models.py                # Pydantic data models
│   ├── config.py                # Configuration management
│   └── analyze_json.py          # JSON structure analyzer tool
│
├── tests/                        # Test suite
│   ├── __init__.py
│   ├── test_config.py           # Configuration tests
│   ├── test_models.py           # Model validation tests
│   ├── test_pipeline.py         # Pipeline logic tests
│   ├── test_integration.py      # End-to-end tests
│   ├── pytest.ini               # Pytest configuration
│   └── run_tests.sh             # Test runner script
│
├── data_input/                   # Input data directory
│   └── o2-product-set.json      # Source JSON file (not in Git)
│
├── output/                       # Generated outputs (created on run)
│   ├── products.json            # Transformed product data
│   ├── products.csv             # Tabular format for analytics
│   └── validation_report.json   # Processing metrics
│
├── logs/                         # Execution logs (created on run)
│   ├── etl_pipeline.log         # Detailed execution trace
│   └── etl_errors.log           # Error-specific log
│
├── README.md                     # Project documentation
├── requirements.txt              # Python dependencies
├── Makefile                      # Build automation
├── Dockerfile                    # Container definition
├── docker-compose.yml            # Docker orchestration
├── .gitignore                    # Git exclusions
└── .dockerignore                 # Docker build exclusions
```

### Directory Descriptions

- **`src/`** - Contains all source code modules for the ETL pipeline
- **`tests/`** - Comprehensive test suite with unit and integration tests
- **`data_input/`** - Place input JSON files here (excluded from Git)
- **`output/`** - Pipeline outputs generated after execution
- **`logs/`** - Execution and error logs for debugging

## Key Implementation Features

### Intelligent Filtering
The pipeline adapts to new products automatically using pattern recognition and structural analysis.

### Robust Validation
Two-tier validation using Pydantic:
- **Input validation**: Ensures data structure integrity
- **Business validation**: Enforces domain rules

### Production-Ready Design
- **Batch processing** for memory efficiency with large files
- **Comprehensive logging** with three-tier strategy (console, file, errors)
- **Error resilience** - continues processing valid records despite failures
- **Environment configuration** - all settings externalized

### Output Flexibility
Multiple output formats for different stakeholders:
- **JSON**: Full structure preservation for APIs
- **CSV**: Flattened representation for BI tools
- **Reports**: Validation metrics and processing statistics

## Quick Start

### Prerequisites
- Python 3.8+
- Docker (optional)

### Installation

```bash
# Clone repository
git clone <https://github.com/vaibhavd030/etl_15g.git>

# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # Unix/Mac

# Install dependencies
pip install -r requirements.txt
```

## Running the Pipeline

### 1. Setup
```bash
# Place your data file in data_input directory
mkdir -p data_input
cp o2-product-set.json data_input/
```

### 2. Analyze Data Structure
```bash
# Understand the JSON structure before processing
make analyze  
# or
python -m src.analyze_json data_input/o2-product-set.json

# Output: Shows field distribution, types, and nesting
```

### 3. Run ETL Pipeline
```bash

# Using Make (recommended)
make run

# Using Python directly
python -m src.pipeline

# Using Docker
docker-compose up
```

### 4. Run Tests
```bash
# All tests
make test

# With coverage
make test-coverage

# Specific test file
pytest tests/test_models.py -v
```

## Expected Output

After running the pipeline, you'll see:

### Console Output
```
======================================================================
O2 ETL PIPELINE INITIALIZED
======================================================================
Input file: data_input/o2-product-set.json
Output directory: output
----------------------------------------------------------------------
PHASE 1: DATA EXTRACTION
----------------------------------------------------------------------
✓ Successfully extracted 83 products
----------------------------------------------------------------------
PHASE 2: DATA TRANSFORMATION
----------------------------------------------------------------------
✓ Transformation complete:
  - Valid products: 83
  - Filtered out: 0
  - Validation errors: 0
----------------------------------------------------------------------
PHASE 3: DATA LOADING
----------------------------------------------------------------------
✓ JSON output saved (37.90 KB) - 83 products
✓ CSV output saved (11.43 KB) - 83 rows
✓ Validation report saved
======================================================================
PIPELINE EXECUTION SUMMARY
======================================================================
✓ Status: SUCCESS
✓ Execution time: 0.21 seconds
✓ Success rate: 100.0%
======================================================================
```

### Generated Files

1. **output/products.json** - Transformed and enriched product data
   - Contains only validated handsets and devices
   - Enriched with extracted storage options, colors, and network technology
   - Structured for API consumption or further processing
   - Each product includes category classification and processing timestamp

2. **output/products.csv** - Flattened tabular format
   - Same products as JSON but in spreadsheet-friendly format
   - Lists converted to pipe-separated strings (e.g., "128GB|256GB|512GB")
   - Ready for Excel, Tableau, or other BI tools
   - Includes all fields: product_id, brand, name, category, sku, stock status, etc.

3. **output/validation_report.json** - Processing metrics and quality report
   - Total records processed vs. successfully validated
   - Brands and categories discovered
   - Validation error details (if any)
   - Processing time and success rate
   - Used for monitoring pipeline health and data quality

4. **logs/etl_pipeline.log** - Detailed execution trace
   - Complete step-by-step execution log
   - Debug information for troubleshooting
   - Timestamp for each operation
   - Error stack traces if failures occur

### Sample Output Data

**products.json:**
```json
{
  "product_id": "9170884379613222327",
  "brand": "Apple",
  "name": "iPhone 16 Pro Max",
  "category": "handset",
  "sku": "MYWX3QN-A",
  "in_stock": true,
  "storage_options": ["256GB", "512GB", "1TB"],
  "color_options": ["Desert Titanium", "White Titanium"],
  "network_technology": "5G",
  "processed_timestamp": "2024-08-24T14:00:00Z"
}
```

**validation_report.json:**
```json
{
  "total_records": 83,
  "valid_records": 83,
  "invalid_records": 0,
  "filtered_records": 0,
  "success_rate": 100.0,
  "brands_processed": ["Apple", "Samsung", "Google", "Motorola"],
  "categories_found": ["handset", "device"],
  "processing_time": 0.21
}
```

## Configuration

Environment variables control pipeline behavior:

| Variable | Description | Default |
|----------|-------------|---------|
| `INPUT_FILE` | Input JSON path | data_input/o2-product-set.json |
| `OUTPUT_DIR` | Output directory | output |
| `BATCH_SIZE` | Records per batch | 1000 |
| `LOG_LEVEL` | Logging verbosity | INFO |

## Data Processing Example

**Input Structure**
```json
{
  "id": "9170884379613222327",
  "brand": "Apple",
  "name": "iPhone 16 Pro Max",
  "deviceOptions": [...],
  "specificationGroups": [...]
}
```

**Output Structure**
```json
{
  "product_id": "9170884379613222327",
  "brand": "Apple",
  "name": "iPhone 16 Pro Max",
  "category": "handset",
  "storage_options": ["256GB", "512GB", "1TB"],
  "color_options": ["Desert Titanium", "White Titanium"],
  "network_technology": "5G",
  "processed_timestamp": "2024-01-15T10:30:00"
}
```

## Performance & Scalability

### Current Performance
- Processes ~1000 products/second
- Memory efficient batch processing
- Validation success rate tracking

### Production Scalability Path
- **Immediate**: Database storage (PostgreSQL/MongoDB) for querying
- **Short-term**: API endpoints for real-time access
- **Long-term**: Distributed processing (Spark/Dask) for TB-scale data
- **Future**: ML-based categorization for improved accuracy

## Monitoring & Observability

The pipeline provides comprehensive monitoring through:

- **Execution Metrics**: Processing time, success rates, category distribution
- **Validation Reports**: Detailed error analysis with failure reasons
- **Structured Logging**: Traceable execution with correlation IDs
- **Health Indicators**: File sizes, record counts, transformation rates

## Success Metrics

### Data Quality KPIs
- Validation success rate: >95%
- Processing speed: <5 seconds per 1000 products
- Category accuracy: >99%

### Business Impact
- Complete coverage of active product catalogue
- Enables real-time pricing decisions
- Supports inventory optimization
- Facilitates competitive analysis

## Technology Decisions

| Technology | Justification |
|------------|--------------|
| **Pydantic** | Type safety with automatic validation and excellent error messages |
| **Docker** | Consistent deployment across environments |
| **JSON/CSV** | Universal formats compatible with downstream systems |
| **Batch Processing** | Scales from small files to multi-GB datasets |

## Testing

The project includes comprehensive tests:
- **Unit Tests**: Test individual components (models, configuration)
- **Integration Tests**: Test end-to-end pipeline flow
- **Performance Tests**: Verify processing speed with large datasets

Run tests with coverage:
```bash
pytest --cov=src --cov-report=html tests/
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Import errors | Ensure virtual environment is activated |
| File not found | Check data_input/ directory for JSON file |
| Permission denied | Run with appropriate permissions or use sudo |
| Memory issues | Reduce BATCH_SIZE in .env file |

---

**Author**: Vaibhav Dikshit  

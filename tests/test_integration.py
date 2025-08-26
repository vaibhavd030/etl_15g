"""
Integration tests for end-to-end pipeline execution.
"""

import json
import csv
from pathlib import Path
import pytest

from src.pipeline import ETLPipeline


class TestEndToEnd:
    """End-to-end integration tests."""
    
    @pytest.fixture
    def test_data_file(self, tmp_path):
        """Create a test data file with realistic data."""
        data = [
            {
                "id": "real_001",
                "brand": "Apple",
                "name": "iPhone 16 Pro Max",
                "code": "IP16PM",
                "skuCode": "MYWX3QN/A",
                "deviceState": "New",
                "inStock": True,
                "averageRating": 4.8,
                "totalReviews": 150,
                "specificationGroups": [
                    {
                        "name": "Network",
                        "specifications": [
                            {"name": "Network Technology", "value": "5G"}
                        ]
                    }
                ],
                "deviceOptions": [
                    {
                        "color": {"name": "Desert Titanium", "hexCode": "#f3cbb4"},
                        "capacityValues": [
                            {"name": "256GB", "deviceCode": "MYWX3QN/A", "isAvailable": True},
                            {"name": "512GB", "deviceCode": "MYX23QN/A", "isAvailable": True}
                        ]
                    }
                ]
            },
            {
                "id": "real_002",
                "brand": "Motorola",
                "name": "moto g35 5G",
                "code": "motog35",
                "deviceState": "New",
                "inStock": False,
                "averageRating": 3.5,
                "totalReviews": 25
            },
            {
                "id": "real_003",
                "brand": "O2",
                "name": "Complete Cover Insurance",
                "code": "INSURANCE",
                "inStock": True
            },
            {
                "id": "real_004",
                "brand": "Generic",
                "name": "Phone Case Premium",
                "code": "ACCESSORY",
                "inStock": True
            }
        ]
        
        test_file = tmp_path / "integration_test.json"
        test_file.write_text(json.dumps(data))
        return test_file
    
    def test_full_pipeline_execution(self, test_data_file, tmp_path):
        """Test complete pipeline execution from file to outputs."""
        # Configure pipeline
        pipeline = ETLPipeline(input_file=str(test_data_file))
        pipeline.output_dir = tmp_path / "output"
        pipeline.output_dir.mkdir(exist_ok=True)
        
        # Run pipeline
        results = pipeline.run()
        
        # Verify execution results
        assert results['status'] == 'success'
        assert results['metrics']['total_records'] == 4
        assert results['metrics']['valid_records'] == 2  # Only phones
        assert results['metrics']['filtered_records'] == 2  # Insurance + accessory
        
        # Verify output files exist
        json_file = Path(results['output_files']['json'])
        csv_file = Path(results['output_files']['csv'])
        report_file = Path(results['output_files']['report'])
        
        assert json_file.exists()
        assert csv_file.exists()
        assert report_file.exists()
        
        # Verify JSON output content
        with open(json_file) as f:
            json_data = json.load(f)
            assert len(json_data) == 2
            assert json_data[0]['brand'] == "Apple"
            assert json_data[0]['category'] == "handset"
            assert "256GB" in json_data[0]['storage_options']
        
        # Verify CSV output content
        with open(csv_file) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 2
            assert rows[0]['brand'] == "Apple"
            assert rows[0]['in_stock'] == "Yes"
        
        # Verify validation report
        with open(report_file) as f:
            report = json.load(f)
            assert report['total_records'] == 4
            assert report['valid_records'] == 2
            assert report['success_rate'] == 50.0
    
    def test_pipeline_with_invalid_data(self, tmp_path):
        """Test pipeline handles invalid data gracefully."""
        # Create file with some invalid data - but ensure some valid ones too
        data = [
            {"id": "valid_001", "brand": "Apple", "name": "iPhone 15", "code": "IP15"},
            {"brand": "Samsung"},  # Missing id and name - will be filtered
            {"id": "valid_002", "brand": "Google", "name": "Pixel 8", "code": "PIX8"},
            {},  # Empty object - will be filtered
            {"id": "valid_003", "brand": "Samsung", "name": "Galaxy S24", "code": "GS24"}
        ]
        
        test_file = tmp_path / "invalid_test.json"
        test_file.write_text(json.dumps(data))
        
        pipeline = ETLPipeline(input_file=str(test_file))
        pipeline.output_dir = tmp_path / "output"
        pipeline.output_dir.mkdir(exist_ok=True)
        
        # Run pipeline - should not crash
        results = pipeline.run()
        
        assert results['status'] == 'success'
        # The invalid records are filtered out, not counted as validation errors
        assert results['metrics']['filtered_records'] == 2  # Empty object and missing id
        assert results['metrics']['valid_records'] == 3  # Three valid phones
        
        # Should still produce output for valid records
        json_file = Path(results['output_files']['json'])
        with open(json_file) as f:
            json_data = json.load(f)
            assert len(json_data) == 3  # Three valid phones
    
    def test_pipeline_performance(self, tmp_path):
        """Test pipeline performance with larger dataset."""
        # Generate 1000 products
        data = []
        for i in range(1000):
            if i % 3 == 0:
                # Handset - ensure it has proper structure
                product = {
                    "id": f"perf_{i:04d}",
                    "brand": "TestBrand",
                    "name": f"iPhone Model {i}",  # Use iPhone to ensure categorization
                    "code": f"IPH{i:04d}",
                    "inStock": i % 2 == 0,
                    "specificationGroups": [{"specifications": []}]
                }
            elif i % 3 == 1:
                # Insurance (should be filtered)
                product = {
                    "id": f"perf_{i:04d}",
                    "brand": "O2",
                    "name": f"Insurance Plan {i}",
                    "code": f"INS{i:04d}",
                    "inStock": True
                }
            else:
                # Accessory (should be filtered)
                product = {
                    "id": f"perf_{i:04d}",
                    "brand": "Generic",
                    "name": f"Phone Case Accessory {i}",
                    "code": f"ACC{i:04d}",
                    "inStock": True
                }
            data.append(product)
        
        test_file = tmp_path / "performance_test.json"
        test_file.write_text(json.dumps(data))
        
        pipeline = ETLPipeline(input_file=str(test_file))
        pipeline.output_dir = tmp_path / "output"
        pipeline.output_dir.mkdir(exist_ok=True)
        
        import time
        start_time = time.time()
        results = pipeline.run()
        execution_time = time.time() - start_time
        
        # Performance assertions
        assert results['status'] == 'success'
        assert execution_time < 5.0  # Should process 1000 records in under 5 seconds
        assert results['metrics']['total_records'] == 1000
        assert results['metrics']['valid_records'] == 334  # ~1/3 are handsets
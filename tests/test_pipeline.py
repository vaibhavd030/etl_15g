"""
Tests for main ETL pipeline functionality.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

from src.pipeline import ETLPipeline
from src.models import Product, TransformedProduct, ValidationReport

class TestETLPipeline:
    """Test ETL pipeline core functionality."""
    
    @pytest.fixture
    def sample_data(self):
        """Fixture providing sample product data."""
        return [
            {
                "id": "001",
                "brand": "Apple",
                "name": "iPhone 15 Pro",
                "deviceState": "New",
                "inStock": True,
                "averageRating": 4.5,
                "specificationGroups": [
                    {
                        "specifications": [
                            {"name": "Network Technology", "value": "5G"}
                        ]
                    }
                ],
                "deviceOptions": [
                    {
                        "color": {"name": "Black"},
                        "capacityValues": [{"name": "256GB"}]
                    }
                ]
            },
            {
                "id": "002",
                "brand": "O2",
                "name": "Device Insurance",
                "inStock": True
            },
            {
                "id": "003",
                "brand": "Samsung",
                "name": "Galaxy S24",
                "deviceState": "New",
                "inStock": False,
                "specificationGroups": []
            }
        ]
    
    @pytest.fixture
    def pipeline(self, tmp_path):
        """Fixture providing configured pipeline instance."""
        test_file = tmp_path / "test.json"
        return ETLPipeline(input_file=str(test_file))
    
    def test_pipeline_initialization(self, pipeline):
        """Test pipeline initializes correctly."""
        assert pipeline.input_file.name == "test.json"
        assert pipeline.output_dir.exists()
        assert pipeline.metrics['total_records'] == 0
        assert pipeline.metrics['valid_records'] == 0
    
    def test_category_filtering(self, pipeline, sample_data):
        """Test product category filtering logic."""
        # iPhone should be included (has specs)
        assert pipeline.should_include_product(sample_data[0]) is True
        
        # Insurance should be excluded
        assert pipeline.should_include_product(sample_data[1]) is False
        
        # Samsung phone should be included (name match)
        assert pipeline.should_include_product(sample_data[2]) is True
    
    def test_extraction_with_valid_file(self, pipeline, sample_data, tmp_path):
        """Test extraction from valid JSON file."""
        # Create test file
        test_file = tmp_path / "test.json"
        test_file.write_text(json.dumps(sample_data))
        
        pipeline.input_file = test_file
        extracted = pipeline.extract()
        
        assert len(extracted) == 3
        assert extracted[0]['brand'] == "Apple"
        assert pipeline.metrics['total_records'] == 3
    
    def test_extraction_with_missing_file(self, pipeline):
        """Test extraction fails gracefully with missing file."""
        pipeline.input_file = Path("nonexistent.json")
        
        with pytest.raises(FileNotFoundError):
            pipeline.extract()
    
    def test_transformation(self, pipeline, sample_data):
        """Test transformation and validation."""
        transformed = pipeline.transform(sample_data)
        
        # Should have 2 products (insurance excluded)
        assert len(transformed) == 2
        assert pipeline.metrics['filtered_records'] == 1
        assert pipeline.metrics['valid_records'] == 2
        
        # Check first product
        iphone = transformed[0]
        assert iphone.brand == "Apple"
        assert iphone.category == "handset"
        assert iphone.network_technology == "5G"
        assert "256GB" in iphone.storage_options
        assert "Black" in iphone.color_options
    
    def test_storage_extraction(self, pipeline):
        """Test extraction of storage options."""
        product_data = {
            "deviceOptions": [
                {
                    "capacityValues": [
                        {"name": "128GB"},
                        {"name": "256GB"},
                        {"name": "512GB"}
                    ]
                }
            ]
        }
        
        storage = pipeline._extract_storage_options(product_data)
        assert len(storage) == 3
        assert "128GB" in storage
        assert "512GB" in storage
    
    def test_network_extraction(self, pipeline):
        """Test extraction of network technology."""
        product_data = {
            "specificationGroups": [
                {
                    "specifications": [
                        {"name": "Battery", "value": "5000mAh"},
                        {"name": "Network Technology", "value": "5G"},
                        {"name": "Display", "value": "6.7 inch"}
                    ]
                }
            ]
        }
        
        network = pipeline._extract_network_technology(product_data)
        assert network == "5G"
    
    def test_category_determination(self, pipeline):
        """Test product category determination logic."""
        # Test handset detection
        assert pipeline._determine_category({"name": "iPhone 15"}) == "handset"
        assert pipeline._determine_category({"name": "Galaxy S24"}) == "handset"
        assert pipeline._determine_category({"name": "Pixel 8"}) == "handset"
        
        # Test tariff detection
        assert pipeline._determine_category({"name": "Monthly Tariff"}) == "pay_monthly"
        assert pipeline._determine_category({"name": "Pay Monthly Plan"}) == "pay_monthly"
        
        # Test default
        assert pipeline._determine_category({"name": "Unknown Product"}) == "device"
    
    @patch('builtins.open', new_callable=mock_open)
    def test_csv_output_generation(self, mock_file, pipeline, tmp_path):
        """Test CSV file generation."""
        # from src.models import TransformedProduct
        from datetime import datetime
        
        products = [
            TransformedProduct(
                product_id="001",
                brand="Apple",
                name="iPhone 15",
                category="handset",
                sku="IP15",
                in_stock=True,
                storage_options=["128GB", "256GB"],
                color_options=["Black"],
                processed_timestamp=datetime.utcnow()
            )
        ]
        
        csv_path = tmp_path / "test.csv"
        pipeline._save_as_csv(products, csv_path)
        
        # Verify CSV was written
        mock_file.assert_called()
        handle = mock_file()
        
        # Check write was called (header + data row)
        assert handle.write.called
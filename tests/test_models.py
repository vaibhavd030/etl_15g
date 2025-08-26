"""
Tests for Pydantic models and validation logic.
"""

import pytest
from datetime import datetime
from pydantic import ValidationError

from src.models import Product, TransformedProduct


class TestProductModel:
    """Test Product model validation."""
    
    def test_valid_product_creation(self):
        """Test creating a valid product."""
        product = Product(
            id="12345",
            brand="Apple",
            name="iPhone 15",
            sku_code="IP15-256",
            in_stock=True,
            average_rating=4.5
        )
        assert product.id == "12345"
        assert product.brand == "Apple"
        assert product.average_rating == 4.5
    
    def test_missing_required_fields(self):
        """Test validation fails for missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            Product(brand="Apple")  # Missing id and name
        
        errors = exc_info.value.errors()
        assert len(errors) == 2
        assert any(e['loc'] == ('id',) for e in errors)
        assert any(e['loc'] == ('name',) for e in errors)
    
    def test_rating_validation(self):
        """Test rating must be between 0 and 5."""
        # Valid rating
        product = Product(
            id="123",
            brand="Samsung",
            name="Galaxy S24",
            average_rating=3.5
        )
        assert product.average_rating == 3.5
        
        # Invalid rating
        with pytest.raises(ValidationError) as exc_info:
            Product(
                id="123",
                brand="Samsung",
                name="Galaxy S24",
                average_rating=6.0  # Too high
            )
        assert "less than or equal to 5" in str(exc_info.value)
    
    def test_auto_sku_generation(self):
        """Test SKU is auto-generated if missing."""
        product = Product(
            id="789",
            brand="Google",
            name="Pixel 8",
            code="PIX8"
        )
        # Should use code as SKU
        assert product.sku_code == "PIX8"
        
        product2 = Product(
            id="999",
            brand="OnePlus",
            name="OnePlus 12"
        )
        # Should generate SKU from brand and id
        assert product2.sku_code == "OnePlus_999"
    
    def test_text_field_cleaning(self):
        """Test text fields are cleaned of whitespace."""
        product = Product(
            id="123",
            brand="  Apple  ",
            name=" iPhone 15 Pro  "
        )
        assert product.brand == "Apple"
        assert product.name == "iPhone 15 Pro"


class TestTransformedProduct:
    """Test TransformedProduct model."""
    
    def test_transformed_product_creation(self):
        """Test creating a transformed product with all fields."""
        product = TransformedProduct(
            product_id="123",
            brand="Apple",
            name="iPhone 15",
            category="handset",
            sku="IP15",
            in_stock=True,
            storage_options=["128GB", "256GB"],
            color_options=["Black", "White"],
            processed_timestamp=datetime.utcnow()
        )
        
        assert product.category == "handset"
        assert len(product.storage_options) == 2
        assert "128GB" in product.storage_options
    
    def test_json_serialization(self):
        """Test product can be serialized to JSON."""
        product = TransformedProduct(
            product_id="456",
            brand="Samsung",
            name="Galaxy S24",
            category="handset",
            sku="GS24",
            in_stock=False,
            processed_timestamp=datetime.utcnow()
        )
        
        data = product.dict()
        assert isinstance(data, dict)
        assert data['product_id'] == "456"
        assert data['in_stock'] is False
        
        # Check timestamp is serializable
        assert isinstance(data['processed_timestamp'], datetime)
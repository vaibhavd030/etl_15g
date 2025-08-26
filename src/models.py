"""
Pydantic models for data validation.

Defines data models for product validation and transformation.
"""

from datetime import datetime
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, validator


class Product(BaseModel):
    """Base product model with validation."""
    
    id: str = Field(..., description="Product ID")
    brand: str = Field(..., description="Brand name")
    name: str = Field(..., description="Product name")
    code: Optional[str] = None
    sku_code: Optional[str] = None
    device_state: Optional[str] = None
    in_stock: bool = False
    average_rating: Optional[float] = Field(None, ge=0, le=5)
    total_reviews: Optional[int] = Field(None, ge=0)
    image_url: Optional[str] = None
    product_url: Optional[str] = None
    
    @validator('brand', 'name')
    def clean_text(cls, v):
        """Clean and validate text fields."""
        if not v or not v.strip():
            raise ValueError("Required field cannot be empty")
        return v.strip()
    
    @validator('sku_code', always=True)
    def ensure_sku(cls, v, values):
        """Ensure SKU exists."""
        if not v:
            if 'code' in values and values['code']:
                return values['code']
            return f"{values.get('brand', 'NA')}_{values.get('id', 'NA')}"
        return v


class TransformedProduct(BaseModel):
    """Transformed product for output."""
    
    product_id: str
    brand: str
    name: str
    category: str
    sku: str
    in_stock: bool
    device_state: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    network_technology: Optional[str] = None
    storage_options: List[str] = Field(default_factory=list)
    color_options: List[str] = Field(default_factory=list)
    processed_timestamp: datetime
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ValidationReport(BaseModel):
    """Validation report model."""
    
    total_records: int
    valid_records: int
    invalid_records: int
    filtered_records: int
    validation_errors: List[Dict[str, Any]]
    processing_time: float
    timestamp: datetime
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_records == 0:
            return 0.0
        return (self.valid_records / self.total_records) * 100
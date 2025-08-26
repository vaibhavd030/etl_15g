"""
Configuration settings for ETL pipeline.
"""

import os
from pathlib import Path
from typing import Set
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    # File paths
    input_file: str = Field(
        default="data_input/o2-product-set.json",
        env="INPUT_FILE"
    )
    output_dir: Path = Field(
        default=Path("output"),
        env="OUTPUT_DIR"
    )
    
    # Processing
    batch_size: int = Field(
        default=1000,
        env="BATCH_SIZE"
    )
    log_level: str = Field(
        default="INFO",
        env="LOG_LEVEL"
    )
    
    # Filtering
    excluded_categories: Set[str] = {
        "insurance", "accessories", "simo", "sim only", 
        "protection", "case", "charger", "cable"
    }
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
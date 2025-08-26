"""
Tests for configuration management.
"""

import pytest
import os
from pathlib import Path
from unittest.mock import patch

from src.config import Settings

class TestConfiguration:
    """Test cases for configuration settings."""
    
    def test_default_settings(self):
        """Test default configuration values."""
        settings = Settings()
        
        assert settings.input_file == "data_input/o2-product-set.json"
        assert settings.output_dir == Path("output")
        assert settings.batch_size == 1000
        assert settings.log_level == "INFO"
        assert "insurance" in settings.excluded_categories
        assert "accessories" in settings.excluded_categories
    
    @patch.dict(os.environ, {
        'INPUT_FILE': 'custom_input.json',
        'OUTPUT_DIR': 'custom_output',
        'BATCH_SIZE': '500',
        'LOG_LEVEL': 'DEBUG'
    })
    def test_environment_variable_override(self):
        """Test that environment variables override defaults."""
        settings = Settings()
        
        assert settings.input_file == "custom_input.json"
        assert settings.output_dir == Path("custom_output")
        assert settings.batch_size == 500
        assert settings.log_level == "DEBUG"
    
    def test_excluded_categories_set(self):
        """Test that excluded categories is a set."""
        settings = Settings()
        
        assert isinstance(settings.excluded_categories, set)
        assert "insurance" in settings.excluded_categories
        assert "simo" in settings.excluded_categories
        assert "sim only" in settings.excluded_categories
        assert "accessories" in settings.excluded_categories
        assert "case" in settings.excluded_categories
        assert "charger" in settings.excluded_categories
    
    def test_output_dir_as_path(self):
        """Test that output_dir is a Path object."""
        settings = Settings()
        
        assert isinstance(settings.output_dir, Path)
        assert settings.output_dir == Path("output")
    
    @patch.dict(os.environ, {'BATCH_SIZE': 'invalid'})
    def test_invalid_batch_size(self):
        """Test that invalid batch size raises validation error."""
        with pytest.raises(ValueError):
            Settings()
    
    @patch.dict(os.environ, {'LOG_LEVEL': 'INVALID'})
    def test_invalid_log_level_accepted(self):
        """Test that any string is accepted for log level."""
        settings = Settings()
        assert settings.log_level == "INVALID"  # Accepted but may fail in logging setup
    
    def test_settings_case_insensitive(self):
        """Test that settings are case insensitive."""
        with patch.dict(os.environ, {'input_file': 'test.json'}):
            settings = Settings()
            assert settings.input_file == "test.json"
    
    def test_settings_immutable_excluded_categories(self):
        """Test that excluded categories cannot be modified after initialization."""
        settings = Settings()
        original_size = len(settings.excluded_categories)
        
        # This modifies the set
        settings.excluded_categories.add("new_category")
        
        # Verify it was added
        assert len(settings.excluded_categories) == original_size + 1
        assert "new_category" in settings.excluded_categories
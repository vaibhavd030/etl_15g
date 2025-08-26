"""
Main ETL pipeline for O2 product catalogue processing.

Orchestrates extraction, transformation, and loading of product data
with comprehensive logging and validation.
"""

import json
import logging
import logging.handlers
import sys
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import traceback

from src.config import settings
from src.models import Product, TransformedProduct, ValidationReport
from pydantic import ValidationError


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for console output."""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging():
    """Configure comprehensive logging with file and console handlers."""
    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, settings.log_level))
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # Console handler with colors
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = ColoredFormatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    
    # File handler for all logs
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / 'etl_pipeline.log',
        maxBytes=10485760,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Error file handler
    error_handler = logging.handlers.RotatingFileHandler(
        log_dir / 'etl_errors.log',
        maxBytes=10485760,  # 10MB
        backupCount=5
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    
    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    
    return logger


# Setup logging
logger = setup_logging()


class ETLPipeline:
    """
    ETL Pipeline for O2 product catalogue processing.
    
    Handles extraction from JSON, transformation with Pydantic validation,
    and loading to multiple output formats.
    """
    
    def __init__(self, input_file: Optional[str] = None):
        """
        Initialize pipeline.
        
        Args:
            input_file: Path to input JSON file
        """
        self.input_file = Path(input_file or settings.input_file)
        self.output_dir = settings.output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize timing
        self.start_time = None
        
        # Metrics
        self.metrics = {
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'filtered_records': 0,
            'validation_errors': [],
            'categories_found': set(),
            'brands_processed': set()
        }
        
        logger.info("=" * 70)
        logger.info("O2 ETL PIPELINE INITIALIZED")
        logger.info("=" * 70)
        logger.info(f"Input file: {self.input_file}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Log level: {settings.log_level}")
        logger.info(f"Batch size: {settings.batch_size}")
        
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from JSON file.
        
        Returns:
            List of raw product dictionaries
        """
        logger.info("-" * 70)
        logger.info("PHASE 1: DATA EXTRACTION")
        logger.info("-" * 70)
        
        if not self.input_file.exists():
            logger.error(f"Input file not found: {self.input_file}")
            raise FileNotFoundError(f"Input file not found: {self.input_file}")
        
        file_size = self.input_file.stat().st_size / (1024 * 1024)  # MB
        logger.info(f"Reading file: {self.input_file.name} ({file_size:.2f} MB)")
        
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                logger.warning("JSON root is not a list, wrapping in list")
                data = [data]
            
            self.metrics['total_records'] = len(data)
            logger.info(f"✓ Successfully extracted {len(data)} products")
            
            # Log sample of brands found
            brands = set(p.get('brand', 'Unknown') for p in data[:100])
            logger.debug(f"Sample brands found: {', '.join(list(brands)[:5])}")
            
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            raise
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            logger.debug(traceback.format_exc())
            raise
    
    def should_include_product(self, product: Dict[str, Any]) -> bool:
        """
        Filter products based on category rules.
        
        Args:
            product: Raw product dictionary
            
        Returns:
            True if product should be included
        """
        # Extract all text that might indicate category
        search_text = ' '.join([
            str(product.get('name', '')),
            str(product.get('code', '')),
            str(product.get('deviceState', '')),
            str(product.get('productType', ''))
        ]).lower()
        
        # Check exclusions first
        for excluded in settings.excluded_categories:
            if excluded.lower() in search_text:
                logger.debug(f"Excluding product {product.get('id')}: contains '{excluded}'")
                return False
        
        # Include handsets (phones) and pay monthly tariffs
        include_terms = ['iphone', 'galaxy', 'pixel', 'moto', 'handset', 
                        'phone', 'tariff', 'pay monthly', '5g', '4g']
        
        # Check if it's a device based on presence of specifications
        has_specs = 'specificationGroups' in product and product['specificationGroups']
        has_device_options = 'deviceOptions' in product and product['deviceOptions']
        
        if has_specs or has_device_options:
            return True
        
        # Check include terms
        for term in include_terms:
            if term in search_text:
                logger.debug(f"Including product {product.get('id')}: matches '{term}'")
                return True
        
        return False
    
    def transform(self, raw_products: List[Dict[str, Any]]) -> List[TransformedProduct]:
        """
        Transform and validate products.
        
        Args:
            raw_products: List of raw product dictionaries
            
        Returns:
            List of transformed and validated products
        """
        logger.info("-" * 70)
        logger.info("PHASE 2: DATA TRANSFORMATION")
        logger.info("-" * 70)
        
        transformed = []
        batch_size = settings.batch_size
        total_batches = (len(raw_products) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(raw_products))
            batch = raw_products[start_idx:end_idx]
            
            logger.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch)} products)")
            
            for raw_product in batch:
                try:
                    # Filter first
                    if not self.should_include_product(raw_product):
                        self.metrics['filtered_records'] += 1
                        continue
                    
                    # Track brand
                    brand = raw_product.get('brand', 'Unknown')
                    self.metrics['brands_processed'].add(brand)
                    
                    # Validate with Pydantic
                    product = Product(
                        id=raw_product.get('id'),
                        brand=brand,
                        name=raw_product.get('name'),
                        code=raw_product.get('code'),
                        sku_code=raw_product.get('skuCode') or raw_product.get('code'),
                        device_state=raw_product.get('deviceState'),
                        in_stock=raw_product.get('inStock', False),
                        average_rating=raw_product.get('averageRating'),
                        total_reviews=raw_product.get('totalReviews'),
                        image_url=raw_product.get('image'),
                        product_url=raw_product.get('url')
                    )
                    
                    # Extract additional info
                    category = self._determine_category(raw_product)
                    self.metrics['categories_found'].add(category)
                    
                    # Create transformed product
                    transformed_product = TransformedProduct(
                        product_id=product.id,
                        brand=product.brand,
                        name=product.name,
                        category=category,
                        sku=product.sku_code,
                        in_stock=product.in_stock,
                        device_state=product.device_state,
                        rating=product.average_rating,
                        review_count=product.total_reviews,
                        network_technology=self._extract_network_technology(raw_product),
                        storage_options=self._extract_storage_options(raw_product),
                        color_options=self._extract_color_options(raw_product),
                        processed_timestamp=datetime.now(timezone.utc)
                    )
                    
                    transformed.append(transformed_product)
                    self.metrics['valid_records'] += 1
                    
                except ValidationError as e:
                    self.metrics['invalid_records'] += 1
                    error_detail = {
                        'product_id': raw_product.get('id', 'unknown'),
                        'product_name': raw_product.get('name', 'unknown'),
                        'errors': e.errors(),
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    self.metrics['validation_errors'].append(error_detail)
                    logger.warning(f"Validation failed for {error_detail['product_id']}: {e.error_count()} errors")
                    
                except Exception as e:
                    self.metrics['invalid_records'] += 1
                    logger.error(f"Transformation error for product {raw_product.get('id')}: {e}")
        
        logger.info(f"✓ Transformation complete:")
        logger.info(f"  - Valid products: {len(transformed)}")
        logger.info(f"  - Filtered out: {self.metrics['filtered_records']}")
        logger.info(f"  - Validation errors: {self.metrics['invalid_records']}")
        logger.info(f"  - Categories found: {', '.join(self.metrics['categories_found'])}")
        logger.info(f"  - Brands processed: {len(self.metrics['brands_processed'])}")
        
        return transformed
    
    def _determine_category(self, product: Dict) -> str:
        """Determine product category."""
        name = product.get('name', '').lower()
        code = product.get('code', '').lower()
        
        if any(term in name or term in code for term in ['iphone', 'galaxy', 'pixel', 'moto']):
            return 'handset'
        elif 'tariff' in name or 'plan' in name:
            return 'pay_monthly'
        return 'device'
    
    def _extract_storage_options(self, product: Dict) -> List[str]:
        """Extract storage options from device options."""
        options = set()
        for device_opt in product.get('deviceOptions', []):
            for capacity in device_opt.get('capacityValues', []):
                if capacity.get('name'):
                    options.add(capacity['name'])
        return sorted(list(options))
    
    def _extract_color_options(self, product: Dict) -> List[str]:
        """Extract color options from device options."""
        options = set()
        for device_opt in product.get('deviceOptions', []):
            if device_opt.get('color', {}).get('name'):
                options.add(device_opt['color']['name'])
        return sorted(list(options))
    
    def _extract_network_technology(self, product: Dict) -> Optional[str]:
        """Extract network technology from specifications."""
        for group in product.get('specificationGroups', []):
            for spec in group.get('specifications', []):
                if 'network' in spec.get('name', '').lower():
                    return spec.get('value')
        return None
    
    def load(self, products: List[TransformedProduct]) -> Dict[str, str]:
        """
        Load transformed products to output files.
        
        Args:
            products: List of transformed products
            
        Returns:
            Paths to output files
        """
        logger.info("-" * 70)
        logger.info("PHASE 3: DATA LOADING")
        logger.info("-" * 70)
        
        # Set start_time if not already set (for standalone load calls)
        if self.start_time is None:
            self.start_time = time.time()
        
        output_paths = {}
        
        if not products:
            logger.warning("No products to load - skipping output generation")
            # Still create empty files for consistency
            json_path = self.output_dir / 'products.json'
            csv_path = self.output_dir / 'products.csv'
            
            with open(json_path, 'w') as f:
                json.dump([], f)
            output_paths['json'] = str(json_path)
            
            with open(csv_path, 'w') as f:
                f.write("product_id,brand,name,category,sku,in_stock,device_state,rating,review_count,network_technology,storage_options,color_options,processed_timestamp\n")
            output_paths['csv'] = str(csv_path)
            
            # Still create report
            report_path = self.output_dir / 'validation_report.json'
            self._save_validation_report(report_path)
            output_paths['report'] = str(report_path)
            
            return output_paths
        
        logger.info(f"Loading {len(products)} products to output files...")
        
        output_paths = {}
        
        # Save as JSON
        json_path = self.output_dir / 'products.json'
        logger.info(f"Writing JSON output to: {json_path}")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(
                [p.model_dump() for p in products],
                f,
                indent=2,
                default=str
            )
        output_paths['json'] = str(json_path)
        file_size = json_path.stat().st_size
        logger.info(f"✓ JSON output saved ({file_size / 1024:.2f} KB) - {len(products)} products")
        
        # Save as CSV
        csv_path = self.output_dir / 'products.csv'
        logger.info(f"Writing CSV output to: {csv_path}")
        self._save_as_csv(products, csv_path)
        output_paths['csv'] = str(csv_path)
        file_size = csv_path.stat().st_size
        
        # Count CSV rows
        with open(csv_path, 'r') as f:
            csv_rows = sum(1 for line in f) - 1  # Subtract header
        logger.info(f"✓ CSV output saved ({file_size / 1024:.2f} KB) - {csv_rows} rows")
        
        # Save validation report
        report_path = self.output_dir / 'validation_report.json'
        logger.info(f"Writing validation report to: {report_path}")
        self._save_validation_report(report_path)
        output_paths['report'] = str(report_path)
        logger.info(f"✓ Validation report saved")
        
        # Save errors if any
        if self.metrics['validation_errors']:
            errors_path = self.output_dir / 'validation_errors.json'
            with open(errors_path, 'w', encoding='utf-8') as f:
                json.dump(self.metrics['validation_errors'], f, indent=2, default=str)
            output_paths['errors'] = str(errors_path)
            logger.warning(f"⚠ Validation errors saved to: {errors_path}")
        
        return output_paths
    
    def _save_as_csv(self, products: List[TransformedProduct], path: Path) -> None:
        """Save products as CSV."""
        import csv
        
        if not products:
            logger.warning("No products to save to CSV")
            return
        
        with open(path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'product_id', 'brand', 'name', 'category', 'sku',
                'in_stock', 'device_state', 'rating', 'review_count',
                'network_technology', 'storage_options', 'color_options',
                'processed_timestamp'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for product in products:
                row = product.model_dump()
                # Convert lists to pipe-separated strings
                if row.get('storage_options'):
                    row['storage_options'] = '|'.join(str(x) for x in row['storage_options'] if x)
                else:
                    row['storage_options'] = ''
                    
                if row.get('color_options'):
                    row['color_options'] = '|'.join(str(x) for x in row['color_options'] if x)
                else:
                    row['color_options'] = ''
                
                # Convert boolean to string
                row['in_stock'] = 'Yes' if row.get('in_stock') else 'No'
                
                # Handle None values
                for key in fieldnames:
                    if row.get(key) is None:
                        row[key] = ''
                
                writer.writerow(row)
    
    def _save_validation_report(self, path: Path) -> None:
        """Save validation report."""
        report = ValidationReport(
            total_records=self.metrics['total_records'],
            valid_records=self.metrics['valid_records'],
            invalid_records=self.metrics['invalid_records'],
            filtered_records=self.metrics['filtered_records'],
            validation_errors=self.metrics['validation_errors'],
            processing_time=time.time() - self.start_time,
            timestamp=datetime.now(timezone.utc)
        )
        
        report_data = report.model_dump()
        report_data['brands_processed'] = list(self.metrics['brands_processed'])
        report_data['categories_found'] = list(self.metrics['categories_found'])
        report_data['success_rate'] = report.success_rate
        
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, default=str)
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            Pipeline execution results
        """
        self.start_time = time.time()
        
        logger.info("=" * 70)
        logger.info("STARTING ETL PIPELINE EXECUTION")
        logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
        logger.info("=" * 70)
        
        try:
            # Extract
            raw_data = self.extract()
            
            # Transform
            transformed_data = self.transform(raw_data)
            
            # Load
            output_paths = self.load(transformed_data)
            
            execution_time = time.time() - self.start_time
            
            # Summary
            logger.info("=" * 70)
            logger.info("PIPELINE EXECUTION SUMMARY")
            logger.info("=" * 70)
            logger.info(f"✓ Status: SUCCESS")
            logger.info(f"✓ Execution time: {execution_time:.2f} seconds")
            logger.info(f"✓ Total records: {self.metrics['total_records']}")
            logger.info(f"✓ Valid products: {self.metrics['valid_records']}")
            logger.info(f"✓ Filtered out: {self.metrics['filtered_records']}")
            logger.info(f"✓ Validation errors: {self.metrics['invalid_records']}")
            logger.info(f"✓ Success rate: {(self.metrics['valid_records'] / self.metrics['total_records'] * 100):.1f}%")
            logger.info(f"✓ Output files:")
            for key, path in output_paths.items():
                logger.info(f"  - {key}: {path}")
            logger.info("=" * 70)
            
            results = {
                'status': 'success',
                'execution_time': round(execution_time, 2),
                'metrics': self.metrics,
                'output_files': output_paths
            }
            
            return results
            
        except Exception as e:
            logger.error("=" * 70)
            logger.error("PIPELINE EXECUTION FAILED")
            logger.error("=" * 70)
            logger.error(f"Error: {e}")
            logger.error(traceback.format_exc())
            raise


def main():
    """Main entry point."""
    try:
        logger.info("Starting O2 ETL Pipeline from command line")
        pipeline = ETLPipeline()
        results = pipeline.run()
        
        # Print summary to console
        print("\n" + "=" * 50)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 50)
        print(f"Execution time: {results['execution_time']} seconds")
        print(f"Products processed: {results['metrics']['valid_records']}")
        print(f"Output files created in: {settings.output_dir}/")
        print("Check logs/ directory for detailed execution logs")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        print("Check logs/etl_errors.log for details")
        return 1


if __name__ == "__main__":
    sys.exit(main())
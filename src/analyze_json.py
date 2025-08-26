#!/usr/bin/env python3
"""
JSON Structure Analyzer for O2 Product Dataset
Maps field hierarchy with types and occurrence counts.
"""

import json
import sys
from pathlib import Path
from collections import Counter


def get_field_type(value):
    """Determine the type of a field value."""
    if value is None:
        return "null"
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "integer"
    elif isinstance(value, float):
        return "float"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        if len(value) == 0:
            return "array[empty]"
        elif isinstance(value[0], dict):
            return "array[object]"
        else:
            return f"array[{get_field_type(value[0])}]"
    elif isinstance(value, dict):
        return "object"
    else:
        return type(value).__name__


def analyze_json(file_path):
    """Analyze JSON file structure with field types and counts."""
    
    print(f"\nAnalyzing: {file_path}")
    print("-" * 50)
    
    # Load the JSON file
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading file: {e}")
        return
    
    # Ensure it's a list
    if not isinstance(data, list):
        data = [data]
    
    total_items = len(data)
    print(f"Total items: {total_items}\n")
    
    # Track field information
    field_info = {}
    
    # Analyze each item
    for item in data:
        for field, value in item.items():
            if field not in field_info:
                field_info[field] = {
                    'count': 0,
                    'types': Counter()
                }
            
            field_info[field]['count'] += 1
            field_info[field]['types'][get_field_type(value)] += 1
            
            # Analyze nested fields for objects and arrays
            if isinstance(value, dict):
                for nested_field, nested_value in value.items():
                    nested_path = f"{field}.{nested_field}"
                    if nested_path not in field_info:
                        field_info[nested_path] = {
                            'count': 0,
                            'types': Counter()
                        }
                    field_info[nested_path]['count'] += 1
                    field_info[nested_path]['types'][get_field_type(nested_value)] += 1
            
            elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                # Analyze first item in array as representative
                for nested_field, nested_value in value[0].items():
                    nested_path = f"{field}[].{nested_field}"
                    if nested_path not in field_info:
                        field_info[nested_path] = {
                            'count': 0,
                            'types': Counter()
                        }
                    field_info[nested_path]['count'] += 1
                    field_info[nested_path]['types'][get_field_type(nested_value)] += 1
    
    # Display results
    print("Field Analysis (name: count [type]):")
    print("=" * 50)
    
    # Sort fields: top-level first, then nested
    top_level = {k: v for k, v in field_info.items() if '.' not in k and '[' not in k}
    nested = {k: v for k, v in field_info.items() if '.' in k or '[' in k}
    
    # Print top-level fields
    for field in sorted(top_level.keys()):
        info = field_info[field]
        type_str = ', '.join(info['types'].keys())
        percentage = (info['count'] / total_items) * 100
        print(f"{field}: {info['count']} ({percentage:.1f}%) [{type_str}]")
    
    # Print nested fields
    if nested:
        print("\nNested Fields:")
        for field in sorted(nested.keys()):
            info = field_info[field]
            type_str = ', '.join(info['types'].keys())
            percentage = (info['count'] / total_items) * 100
            indent = "  " * (field.count('.') + field.count('['))
            print(f"{indent}{field}: {info['count']} ({percentage:.1f}%) [{type_str}]")
    
    # Prepare output for JSON
    output = {
        "total_items": total_items,
        "field_analysis": {}
    }
    
    for field, info in field_info.items():
        output["field_analysis"][field] = {
            "count": info['count'],
            "percentage": round((info['count'] / total_items) * 100, 2),
            "types": list(info['types'].keys()),
            "present_in_all": info['count'] == total_items
        }
    
    # Save to JSON
    with open('json_analysis_summary.json', 'w') as f:
        json.dump(output, f, indent=2)
    
    print("\n" + "=" * 50)
    print("Fields present in ALL items:")
    for field, info in field_info.items():
        if info['count'] == total_items and '.' not in field and '[' not in field:
            type_str = ', '.join(info['types'].keys())
            print(f"  {field}: {info['count']} [{type_str}]")
    
    print(f"\nDetailed analysis saved to json_analysis_summary.json")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_json.py <json_file>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    if not Path(file_path).exists():
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    
    analyze_json(file_path)


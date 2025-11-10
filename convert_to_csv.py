#!/usr/bin/env python3
"""
Convert JSON data files to CSV format for dbt seeds
"""
import json
import csv
import os

def json_to_csv(json_file_path, csv_file_path, id_field):
    """Convert JSON array to CSV file"""
    print(f"Converting {json_file_path} to {csv_file_path}...")
    
    # Read JSON file
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)
    
    if not data:
        print(f"No data found in {json_file_path}")
        return
    
    # Get field names from first record
    fieldnames = list(data[0].keys())
    
    # Write CSV file
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        
        for record in data:
            # Handle null values and empty strings for numeric fields
            clean_record = {}
            for key, value in record.items():
                if value is None:
                    clean_record[key] = ""  # Convert null to empty string
                elif key == 'company_revenue' and value == '':
                    clean_record[key] = ""  # Keep empty string for revenue
                else:
                    clean_record[key] = value
            writer.writerow(clean_record)
    
    print(f"✓ Converted {len(data)} records to {csv_file_path}")

def main():
    # Define file paths
    base_dir = "/Users/mahmoud/Workspace/finny"
    seeds_dir = os.path.join(base_dir, "dbt_service", "seeds")
    
    # Convert FXF data
    fxf_json_path = os.path.join(base_dir, "fxf_data.json")
    fxf_csv_path = os.path.join(seeds_dir, "fxf_data.csv")
    
    # Convert PDL data
    pdl_json_path = os.path.join(base_dir, "pdl_data.json")
    pdl_csv_path = os.path.join(seeds_dir, "pdl_data.csv")
    
    # Create seeds directory if it doesn't exist
    os.makedirs(seeds_dir, exist_ok=True)
    
    # Convert files
    json_to_csv(fxf_json_path, fxf_csv_path, "fxf_id")
    json_to_csv(pdl_json_path, pdl_csv_path, "pdl_id")
    
    print("\n✓ All conversions completed!")
    print(f"CSV files created in: {seeds_dir}")

if __name__ == "__main__":
    main()
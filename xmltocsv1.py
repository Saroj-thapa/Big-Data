"""
Quick converter for WBSV XML files - runs xml_to_csv_converter with your folder
"""
import sys
from xml_to_csv_converter import XMLToBusCSVConverter

# YOUR FOLDER PATH
FOLDER_PATH = r"data"

print(f"Converting XML files from: {FOLDER_PATH}\n")

# Create converter
converter = XMLToBusCSVConverter(FOLDER_PATH)

# Process all XML files
record_count = converter.process_all_files()

if record_count > 0:
    # Print summary
    converter.print_summary()
    
    # Save to CSV
    output_path = converter.save_to_csv()
    
    if output_path:
        print(f"\n✓ Conversion complete!")
        print(f"  Records: {len(converter.extracted_data)}")
        print(f"  Columns: {len(converter.field_names)}")
        print(f"  CSV saved to: {output_path}")
else:
    print("\n✗ No data extracted. Check XML file format.")
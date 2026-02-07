"""
Comprehensive XML to CSV Converter for Bus Data
Dynamically extracts ALL available fields regardless of XML structure
Supports both folder and ZIP file inputs
"""

import xml.etree.ElementTree as ET
import csv
import os
import zipfile
import tempfile
import shutil
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple, Any, Set


class XMLToBusCSVConverter:
    """
    Flexible XML to CSV converter that discovers and extracts all available fields
    from bus data XML files, handling various data structures and completeness levels.
    """
    
    def __init__(self, xml_folder: str = None):
        """
        Initialize the converter.
        
        Args:
            xml_folder: Path to folder containing XML files. If None, uses current directory.
        """
        self.xml_folder = xml_folder or os.getcwd()
        self.extracted_data = []
        self.field_names = set(['source_file'])  # Always include source_file
        self.field_hierarchy = {}
        self.data_quality = defaultdict(int)
        self.source_files = []
    
    def _strip_namespace(self, tag: str) -> str:
        """Remove XML namespace from tag name."""
        if '}' in tag:
            return tag.split('}')[1]
        return tag
        
    def find_xml_files(self) -> List[str]:
        """Find all XML files in the specified folder and all subfolders."""
        xml_files = list(Path(self.xml_folder).glob("**/*.xml"))
        self.source_files = [str(f) for f in xml_files]
        return self.source_files
    
    def _get_element_path(self, element, parent_path: str = "") -> str:
        """Generate hierarchical path for element (for column naming)."""
        tag_clean = self._strip_namespace(element.tag)
        current_path = f"{parent_path}.{tag_clean}" if parent_path else tag_clean
        return current_path
    
    def _extract_element_data(self, element, parent_path: str = "", depth: int = 0) -> Dict[str, Any]:
        """
        Recursively extract all data from an XML element.
        
        Returns a flat dictionary with hierarchical keys.
        """
        data = {}
        current_path = self._get_element_path(element, parent_path)
        
        # Extract attributes
        for attr_name, attr_value in element.attrib.items():
            attr_name_clean = self._strip_namespace(attr_name)
            key = f"{current_path}.@{attr_name_clean}"
            data[key] = attr_value
            self.field_names.add(key)
        
        # Extract text content
        if element.text and element.text.strip():
            data[current_path] = element.text.strip()
            self.field_names.add(current_path)
        
        # Process children
        children = list(element)
        if children:
            # Group children by tag to handle multiple occurrences
            children_by_tag = defaultdict(list)
            for child in children:
                children_by_tag[child.tag].append(child)
            
            # For single-occurrence children, extract directly
            for tag, child_list in children_by_tag.items():
                if len(child_list) == 1:
                    child_data = self._extract_element_data(
                        child_list[0], current_path, depth + 1
                    )
                    data.update(child_data)
        
        return data
    
    def _flatten_nested_records(self, element, parent_path: str = "", 
                                parent_data: Dict = None) -> List[Dict]:
        """
        Extract records handling various nesting patterns:
        - Stop-to-stop connections
        - Individual stop records
        - Journey/route records
        - Schedule entries
        """
        if parent_data is None:
            parent_data = {}
        
        records = []
        current_path = self._get_element_path(element, parent_path)
        
        # Extract this element's direct data
        current_data = parent_data.copy()
        
        # Add attributes
        for attr_name, attr_value in element.attrib.items():
            attr_name_clean = self._strip_namespace(attr_name)
            key = f"{self._strip_namespace(element.tag)}.@{attr_name_clean}"
            current_data[key] = attr_value
            self.field_names.add(key)
        
        # Add text content
        if element.text and element.text.strip():
            key = f"{self._strip_namespace(element.tag)}.value"
            current_data[key] = element.text.strip()
            self.field_names.add(key)
        
        # Get children
        children = list(element)
        
        if not children:
            # Leaf node - this is a record
            if current_data:
                records.append(current_data)
        else:
            # Determine if this is a container or record level
            # Check if children are all the same type (indicates collection)
            child_tags = [c.tag for c in children]
            same_tag = len(set(child_tags)) == 1
            
            if same_tag and len(children) > 1:
                # This is a collection - extract each child as a record
                for child in children:
                    child_records = self._flatten_nested_records(
                        child, current_path, current_data
                    )
                    records.extend(child_records)
            else:
                # These are different types of children - continue nesting
                for child in children:
                    child_records = self._flatten_nested_records(
                        child, current_path, current_data
                    )
                    records.extend(child_records)
        
        return records
    
    def process_xml_file(self, filepath: str) -> Tuple[List[Dict], int]:
        """
        Process a single XML file and extract all data.
        
        Returns:
            Tuple of (records, record_count)
        """
        try:
            tree = ET.parse(filepath)
            root = tree.getroot()
            
            filename = Path(filepath).name
            print(f"\n{'='*70}")
            print(f"Processing: {filename}")
            print(f"Root element: {root.tag}")
            print(f"{'='*70}")
            
            # Strategy 1: Try to find obvious record collections
            records = self._extract_records_smart(root, filepath)
            
            if not records:
                # Fallback: Extract all nested data
                print("  → Using fallback extraction strategy...")
                records = self._flatten_nested_records(root)
            
            # Ensure all records have source_file
            for record in records:
                if 'source_file' not in record:
                    record['source_file'] = filename
            
            print(f"  ✓ Extracted {len(records)} records")
            return records, len(records)
            
        except ET.ParseError as e:
            print(f"  ✗ Error parsing XML: {e}")
            return [], 0
        except Exception as e:
            print(f"  ✗ Unexpected error: {e}")
            return [], 0
    
    def _extract_records_smart(self, root, filepath: str) -> List[Dict]:
        """
        Smart extraction strategy that looks for common bus data patterns.
        Attempts to identify and extract stop-to-stop, route, or journey records.
        """
        filename = Path(filepath).name
        records = []
        
        # Strategy 1: Look for stop-to-stop journey/connection records
        journey_patterns = ['Journey', 'Leg', 'Segment', 'Connection', 'Route', 
                           'Stop', 'Station', 'Transit']
        
        for pattern in journey_patterns:
            elements = root.findall(f".//{pattern}")
            if elements and len(elements) > 1:
                print(f"  → Found {len(elements)} '{pattern}' elements")
                for elem in elements:
                    record = self._element_to_record(elem, filepath)
                    if record:
                        records.append(record)
                if records:
                    return records
        
        # Strategy 2: Look for nested collections
        for child in root:
            # Check if this child has multiple identical children
            grandchildren = list(child)
            if grandchildren:
                gc_tags = [gc.tag for gc in grandchildren]
                if len(set(gc_tags)) == 1 and len(grandchildren) > 1:
                    # Found a collection
                    print(f"  → Found collection of {len(grandchildren)} '{grandchildren[0].tag}' elements")
                    for gc in grandchildren:
                        record = self._element_to_record(gc, filepath)
                        if record:
                            records.append(record)
                    if records:
                        return records
        
        # Strategy 3: If root has multiple identical children
        children = list(root)
        if children:
            child_tags = [c.tag for c in children]
            if len(set(child_tags)) == 1 and len(children) > 1:
                print(f"  → Found {len(children)} root-level '{children[0].tag}' elements")
                for child in children:
                    record = self._element_to_record(child, filepath)
                    if record:
                        records.append(record)
                return records
        
        return records
    
    def _element_to_record(self, element, filepath: str) -> Dict[str, Any]:
        """Convert an XML element to a flat dictionary record."""
        record = {'source_file': Path(filepath).name}
        self._flatten_element(element, record)
        return record if len(record) > 1 else {}  # Return only if has data beyond filename
    
    def _flatten_element(self, element, record: Dict, prefix: str = ""):
        """Recursively flatten element into record dictionary."""
        # Add attributes
        for attr_name, attr_value in element.attrib.items():
            attr_name_clean = self._strip_namespace(attr_name)
            key = f"{prefix}{self._strip_namespace(element.tag)}_@{attr_name_clean}" if prefix else f"{self._strip_namespace(element.tag)}_@{attr_name_clean}"
            record[key] = attr_value
            self.field_names.add(key)
        
        # Add text content
        if element.text and element.text.strip():
            key = f"{prefix}{self._strip_namespace(element.tag)}" if prefix else self._strip_namespace(element.tag)
            record[key] = element.text.strip()
            self.field_names.add(key)
        
        # Process children
        for child in element:
            new_prefix = f"{prefix}{self._strip_namespace(element.tag)}_" if prefix else f"{self._strip_namespace(element.tag)}_"
            self._flatten_element(child, record, new_prefix)
    
    def process_all_files(self) -> int:
        """Process all XML files in the folder."""
        xml_files = self.find_xml_files()
        
        if not xml_files:
            print(f"No XML files found in: {self.xml_folder}")
            return 0
        
        print(f"\nFound {len(xml_files)} XML file(s)")
        
        total_records = 0
        for filepath in xml_files:
            records, count = self.process_xml_file(filepath)
            self.extracted_data.extend(records)
            total_records += count
        
        return total_records
    
    def save_to_csv(self, output_file: str = None) -> str:
        """
        Save extracted data to CSV file.
        
        Args:
            output_file: Output CSV filename. If None, auto-generates from XML filename.
        
        Returns:
            Path to created CSV file
        """
        if not self.extracted_data:
            print("\nNo data to save!")
            return ""
        
        # Ensure all records have source_file field (fallback)
        for record in self.extracted_data:
            if 'source_file' not in record:
                record['source_file'] = 'unknown'
        
        # Auto-generate output filename if not provided
        if output_file is None:
            if len(self.source_files) == 1:
                base_name = Path(self.source_files[0]).stem
                output_file = f"{base_name}_converted.csv"
            else:
                output_file = "bus_data_combined.csv"
        
        # Ensure .csv extension
        if not output_file.endswith('.csv'):
            output_file += '.csv'
        
        output_path = os.path.join(self.xml_folder, output_file)
        
        # Sort field names for consistent column order
        sorted_fields = sorted(list(self.field_names))
        
        # Prioritize key fields at the beginning
        priority_fields = [
            'source_file',
            'Route', 'RouteID', 'LineID', 'LineName', 'Line',
            'FromStop', 'ToStop', 'StopID', 'StopName', 'Station', 'Stop',
            'AnnotatedStopPointRef', 'CommonName', 'Indicator', 'LocalityName', 'StopPointRef',
            'Lat', 'Latitude', 'Lon', 'Longitude', 'Coordinates',
            'Runtime', 'Duration', 'TravelTime', 'Time', 'Departure', 'Arrival',
            'Timestamp', 'Date', 'Time',
            'Direction', 'Sequence',
            'Operator', 'Company', 'Provider'
        ]
        
        # Build column order
        ordered_fields = []
        for field in priority_fields:
            if field in sorted_fields:
                ordered_fields.append(field)
                sorted_fields.remove(field)
        
        # Add remaining fields
        ordered_fields.extend(sorted_fields)
        
        # Write CSV
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=ordered_fields)
                writer.writeheader()
                writer.writerows(self.extracted_data)
            
            print(f"\n✓ CSV saved to: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"✗ Error saving CSV: {e}")
            return ""
    
    def print_summary(self):
        """Print extraction summary and data quality report."""
        print(f"\n{'='*70}")
        print("EXTRACTION SUMMARY")
        print(f"{'='*70}")
        
        # Files processed
        print(f"\nFiles Processed: {len(self.source_files)}")
        for f in self.source_files:
            print(f"  • {Path(f).name}")
        
        # Records extracted
        print(f"\nRecords Extracted: {len(self.extracted_data)}")
        
        # Fields found
        print(f"\nFields Found: {len(self.field_names)}")
        print("\nField List:")
        for i, field in enumerate(sorted(self.field_names), 1):
            print(f"  {i:3d}. {field}")
        
        # Data quality analysis
        print(f"\n{'='*70}")
        print("DATA QUALITY ANALYSIS")
        print(f"{'='*70}")
        
        if self.extracted_data:
            total_records = len(self.extracted_data)
            
            # High priority fields analysis
            high_priority = {
                'Stop Names/IDs': ['FromStop', 'ToStop', 'StopID', 'StopName', 'Station', 'Stop'],
                'Coordinates': ['Lat', 'Latitude', 'Lon', 'Longitude', 'Coordinates'],
                'Travel Time': ['Runtime', 'Duration', 'TravelTime'],
                'Route Info': ['Route', 'RouteID', 'LineID', 'LineName', 'Line'],
                'Time Info': ['Departure', 'Arrival', 'Time', 'Timestamp', 'Date']
            }
            
            print("\nHigh Priority Fields Coverage:")
            for category, fields in high_priority.items():
                count = 0
                for record in self.extracted_data:
                    if any(record.get(f) for f in fields):
                        count += 1
                percentage = (count / total_records * 100) if total_records > 0 else 0
                status = "✓" if percentage > 50 else "⚠" if percentage > 0 else "✗"
                print(f"  {status} {category}: {count}/{total_records} ({percentage:.1f}%)")
            
            # Sample record
            print(f"\nSample Record (first row):")
            if self.extracted_data:
                for key, value in list(self.extracted_data[0].items())[:10]:
                    print(f"  {key}: {value}")
                if len(self.extracted_data[0]) > 10:
                    print(f"  ... and {len(self.extracted_data[0]) - 10} more fields")


def main():
    """Main execution function."""
    import argparse
    import sys
    
    # ===== CHANGE THIS PATH TO YOUR ZIP FILE OR FOLDER =====
    INPUT_PATH = r"data"  # Example: "bus_data.zip" or "bus_data_folder"
    # If INPUT_PATH is a ZIP file, it will be extracted automatically
    # If INPUT_PATH is a folder, it will be used directly
    # ========================================================
    
    parser = argparse.ArgumentParser(
        description="Convert bus data XML files to CSV format (supports ZIP files and folders)"
    )
    parser.add_argument(
        '--input',
        type=str,
        default=INPUT_PATH,
        help="Path to ZIP file or folder containing XML files"
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help="Output CSV filename (default: auto-generated)"
    )
    
    args = parser.parse_args([])
    
    # Check if input is a ZIP file
    if args.input.endswith('.zip') and os.path.isfile(args.input):
        print(f"ZIP file detected: {args.input}")
        print("Extracting ZIP file...")
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp()
        print(f"Temporary extraction folder: {temp_dir}")
        
        try:
            # Extract ZIP file
            with zipfile.ZipFile(args.input, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            print(f"✓ ZIP file extracted successfully")
            
            # Get the output directory (use input file's directory)
            output_dir = os.path.dirname(args.input)
            
            # Process the extracted files
            converter = XMLToBusCSVConverter(temp_dir)
            record_count = converter.process_all_files()
            
            if record_count > 0:
                # Print summary
                converter.print_summary()
                
                # Save to CSV in the original input directory
                if args.output is None:
                    args.output = "bus_data_from_zip.csv"
                
                output_path = os.path.join(output_dir, args.output)
                
                # Save CSV
                if not args.output.endswith('.csv'):
                    output_path += '.csv'
                
                # Temporarily override the xml_folder for saving
                converter.xml_folder = output_dir
                converter.save_to_csv(os.path.basename(output_path))
                
                print(f"\n✓ Conversion complete!")
                print(f"  Records: {len(converter.extracted_data)}")
                print(f"  Columns: {len(converter.field_names)}")
            else:
                print("\n✗ No data extracted from ZIP file. Check XML file format and try again.")
        
        except zipfile.BadZipFile:
            print(f"\n✗ Error: The ZIP file is corrupted or invalid")
            print(f"  File: {args.input}")
            print(f"\nPossible solutions:")
            print(f"  1. Re-download or re-compress the ZIP file")
            print(f"  2. If the file is already extracted, use the folder path instead")
            print(f"  3. Check if the file was uploaded correctly to Google Drive")
            shutil.rmtree(temp_dir, ignore_errors=True)
            return
        
        except Exception as e:
            print(f"\n✗ Error extracting ZIP file: {e}")
            shutil.rmtree(temp_dir, ignore_errors=True)
            return
        
        finally:
            # Clean up temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)
            print(f"\n✓ Temporary files cleaned up")
    
    elif os.path.isdir(args.input):
        # Process folder directly
        print(f"Folder detected: {args.input}")
        converter = XMLToBusCSVConverter(args.input)
        record_count = converter.process_all_files()
        
        if record_count > 0:
            # Print summary
            converter.print_summary()
            
            # Save to CSV
            output_path = converter.save_to_csv(args.output)
            
            if output_path:
                print(f"\n✓ Conversion complete!")
                print(f"  Records: {len(converter.extracted_data)}")
                print(f"  Columns: {len(converter.field_names)}")
        else:
            print("\n✗ No data extracted. Check XML file format and try again.")
    
    else:
        print(f"✗ Error: Input path not found or not a valid ZIP file/folder")
        print(f"  Path: {args.input}")


if __name__ == "__main__":
    main()
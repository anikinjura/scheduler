#!/usr/bin/env python3
# Script to create a detailed directory structure with sample files
# that mimics the camera archive structure for a specific PVZ based on the camera configuration,
# with configurable date ranges and hours.
#
# This script accepts all required parameters as command-line arguments, 
# with no configuration file reading.
#
# The structure follows these patterns:
# 1. For UNV cameras (local): unv_camera\[location]\YYYYMMDD\HH
# 2. For UNV cameras (network): [PVZ_ID]\unv_camera\[location]\YYYYMMDD\HH
# 3. For Xiaomi cameras (local): xiaomi_camera_videos\[uid]\YYYYMMDDHH
# 4. For Xiaomi cameras (network): [PVZ_ID]\xiaomi_camera_videos\[uid]\YYYYMMDDHH
#
# The script uses:
# - All parameters provided as command-line arguments

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import argparse
import random
import json


def create_directory(dir_path):
    """Create a directory at the specified path."""
    try:
        os.makedirs(dir_path, exist_ok=True)
        # print(f"Created directory: {dir_path}")
    except Exception as e:
        print(f"Error creating directory {dir_path}: {e}")


def create_sample_file(file_path):
    """Create a small sample file (just a placeholder)."""
    try:
        with open(file_path, 'wb') as f:
            # Create a small file with some content to simulate a file
            f.write(b"SIMULATED_FILE_CONTENT")
        # print(f"Created sample file: {file_path}")
    except Exception as e:
        print(f"Error creating file {file_path}: {e}")


def parse_date(date_str):
    """Parse date string in YYYYMMDD format."""
    return datetime.strptime(date_str, "%Y%m%d")


def generate_date_range(start_date, end_date):
    """Generate a list of dates between start_date and end_date (inclusive)."""
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
    return dates


def create_sample_unv_files(hour_path, date, hour):
    """
    Create sample files for UNV cameras in the format HH-MM-SS.jpg
    
    Args:
        hour_path (str): Path to the hour directory
        date (datetime): Date for the files
        hour (int): Hour for the files
    """
    # Create 5-10 sample files per hour for UNV cameras
    num_files = random.randint(5, 10)
    
    for i in range(num_files):
        # Generate random minutes and seconds
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        # Create filename in format HH-MM-SS.jpg
        filename = f"{hour:02d}-{minute:02d}-{second:02d}.jpg"
        file_path = os.path.join(hour_path, filename)
        create_sample_file(file_path)


def create_sample_xiaomi_files(datetime_path, date, hour):
    """
    Create sample files for Xiaomi cameras in the format 00M21S_1741672821.mp4
    
    Args:
        datetime_path (str): Path to the datetime directory
        date (datetime): Date for the files
        hour (int): Hour for the files
    """
    # Create 3-7 sample files per hour for Xiaomi cameras
    num_files = random.randint(3, 7)
    
    for i in range(num_files):
        # Generate random minutes and seconds
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        # Generate a random timestamp-like number
        timestamp = random.randint(1741672821, 1741679999)
        
        # Create filename in format 00M21S_1741672821.mp4
        filename = f"{minute:02d}M{second:02d}S_{timestamp}.mp4"
        file_path = os.path.join(datetime_path, filename)
        create_sample_file(file_path)


def create_detailed_camera_structure(
    root_path, pvz_id, 
    start_date_str, end_date_str, 
    start_hour, end_hour,
    cameras_config,  # Camera configuration passed as a dictionary
    include_pvz_in_path=True  # Whether to include PVZ ID in the path (True for network, False for local)
):
    """
    Create a detailed directory structure with sample files for a specific PVZ.
    
    Args:
        root_path (str): Root directory for the structure
        pvz_id (int): PVZ ID to process
        start_date_str (str): Start date in YYYYMMDD format
        end_date_str (str): End date in YYYYMMDD format
        start_hour (int): Start hour (0-23)
        end_hour (int): End hour (0-23)
        cameras_config (dict): Camera configuration in format {zone_name: [{'id': str, 'uid': str, 'локация': str}, ...]}
        include_pvz_in_path (bool): Whether to include PVZ ID in the path (True for network, False for local)
    """
    print(f"Creating detailed camera structure for PVZ {pvz_id} at: {root_path}")
    print(f"Date range: {start_date_str} to {end_date_str}")
    print(f"Hour range: {start_hour:02d} to {end_hour:02d}")
    print(f"Include PVZ in path: {include_pvz_in_path}")
    
    # Parse dates
    start_date = parse_date(start_date_str)
    end_date = parse_date(end_date_str)
    
    # Generate date range
    dates = generate_date_range(start_date, end_date)
    
    # Determine the base path depending on whether PVZ ID should be included
    if include_pvz_in_path:
        # For network paths: [root_path]/[PVZ_ID]
        pvz_path = os.path.join(root_path, str(pvz_id))
        print(f"Processing PVZ {pvz_id} with PVZ ID in path...")
    else:
        # For local paths: [root_path] (PVZ ID not included)
        pvz_path = root_path
        print(f"Processing local cameras for PVZ {pvz_id} without PVZ ID in path...")
    
    # Create camera type directories
    unv_camera_path = os.path.join(pvz_path, "unv_camera")
    xiaomi_camera_path = os.path.join(pvz_path, "xiaomi_camera_videos")
    create_directory(unv_camera_path)
    create_directory(xiaomi_camera_path)
    
    # Process cameras by zones
    for zone_name, cameras in cameras_config.items():
        # Map Russian zone names to English for directory naming
        if zone_name == "склад":
            english_zone = "sklad"
        elif zone_name == "клиентская зона":
            english_zone = "client_area"
        else:
            # For any other zone names, replace spaces with underscores
            english_zone = zone_name.replace(' ', '_')
        
        print(f"  Processing zone: {zone_name} ({english_zone})")
        
        # Process each camera in the zone
        for camera in cameras:
            camera_id = camera['id']
            camera_uid = camera['uid']
            
            print(f"    Processing camera: {camera_id} (type: {camera_id.split('_')[0]}) with UID: {camera_uid}")
            
            if camera_id.startswith('unv'):
                # UNV camera structure: [unv_camera_path]\[location]\YYYYMMDD\HH
                unv_zone_path = os.path.join(unv_camera_path, english_zone)
                create_directory(unv_zone_path)
                
                # Create date and hour structure with sample files
                for date in dates:
                    date_str = date.strftime("%Y%m%d")
                    date_path = os.path.join(unv_zone_path, date_str)
                    create_directory(date_path)
                    
                    # Create hour directories with sample files
                    for hour in range(start_hour, end_hour + 1):
                        hour_path = os.path.join(date_path, f"{hour:02d}")
                        create_directory(hour_path)
                        
                        # Create sample files for UNV cameras
                        create_sample_unv_files(hour_path, date, hour)
                        
            elif camera_id.startswith('xiaomi'):
                # Xiaomi camera structure: [xiaomi_camera_path]\[uid]\YYYYMMDDHH
                camera_path = os.path.join(xiaomi_camera_path, camera_uid)
                create_directory(camera_path)
                
                # Create datetime directories with sample files
                for date in dates:
                    date_str = date.strftime("%Y%m%d")
                    for hour in range(start_hour, end_hour + 1):
                        hour_str = f"{hour:02d}"
                        datetime_path = os.path.join(camera_path, f"{date_str}{hour_str}")
                        create_directory(datetime_path)
                        
                        # Create sample files for Xiaomi cameras
                        create_sample_xiaomi_files(datetime_path, date, hour)


def main():
    """Main function to create the detailed camera structure with sample files."""
    parser = argparse.ArgumentParser(description="Create detailed camera directory structure with sample files for a specific PVZ based on camera configuration passed as JSON file")
    parser.add_argument("--start-date", default="20250915", help="Start date in YYYYMMDD format (default: 20250915)")
    parser.add_argument("--end-date", default="20250920", help="End date in YYYYMMDD format (default: 20250920)")
    parser.add_argument("--start-hour", type=int, default=9, help="Start hour (0-23, default: 9)")
    parser.add_argument("--end-hour", type=int, default=21, help="End hour (0-23, default: 21)")
    parser.add_argument("--output-dir", required=True, help="Output directory path (required)")
    parser.add_argument("--pvz-id", type=int, required=True, help="PVZ ID to process (required)")
    parser.add_argument("--include-pvz-id", action="store_true", help="Include PVZ ID in the directory structure (for network paths); omit for local paths")
    
    # Argument for camera configurations JSON file
    parser.add_argument("--cameras-config-file", required=True, help="Path to JSON file containing camera configurations")
    parser.add_argument("--force", action="store_true", help="Force overwrite without confirmation")
    
    args = parser.parse_args()
    
    # Validate hour ranges
    if not (0 <= args.start_hour <= 23) or not (0 <= args.end_hour <= 23):
        print("Error: Hours must be between 0 and 23")
        return
    
    if args.start_hour > args.end_hour:
        print("Error: Start hour must be less than or equal to end hour")
        return
    
    # Load camera configurations from JSON file
    try:
        with open(args.cameras_config_file, 'r', encoding='utf-8') as f:
            cameras_config = json.load(f)
    except Exception as e:
        print(f"Error reading camera configurations from JSON file: {e}")
        return
    
    # Define root directory for the structure
    root_path = args.output_dir
    
    # Check if structure already exists
    if os.path.exists(root_path):
        if args.force:
            should_overwrite = True
        else:
            response = input(f"Camera structure directory '{root_path}' already exists. Overwrite? (y/N): ")
            should_overwrite = response.lower() == 'y'
        
        if not should_overwrite:
            print("Operation cancelled.")
            return
    
    # Create the detailed camera structure with sample files for the specific PVZ
    create_detailed_camera_structure(
        root_path, 
        args.pvz_id,
        args.start_date, 
        args.end_date, 
        args.start_hour, 
        args.end_hour,
        cameras_config,
        include_pvz_in_path=args.include_pvz_id
    )
    
    print(f"\nDetailed camera structure with sample files for PVZ {args.pvz_id} created successfully at: {root_path}")


if __name__ == "__main__":
    main()
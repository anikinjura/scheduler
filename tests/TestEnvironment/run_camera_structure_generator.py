#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to read parameters from configuration files and pass them as arguments 
to create_detailed_camera_structure_standalone.py using a JSON file for camera configurations
"""

import sys
import subprocess
from pathlib import Path
import json
import argparse
from datetime import datetime, timedelta

# Add the project root directory to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.base_config import PVZ_ID, ENV_MODE, PATH_CONFIG, BASE_DIR, LOGS_DIR, SCHEDULER_RUNNER_DIR, TASKS_DIR
from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS, CAMERAS_LOCAL, CAMERAS_NETWORK, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
from scheduler_runner.tasks.cameras.config.cameras_list import CAMERAS_BY_PVZ


def get_default_datetime():
    """Get default datetime - the previous hour from current system time."""
    now = datetime.now()
    prev_hour = now - timedelta(hours=1)
    
    # Format as YYYYMMDD string and extract hour
    date_str = prev_hour.strftime("%Y%m%d")
    hour = prev_hour.hour
    
    return date_str, hour


def setup_argument_parser():
    """Setup command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Run camera structure generator with optional date range. "
                   "If no dates are provided, defaults to the previous hour."
    )
    parser.add_argument(
        "--start-date",
        help="Start date in YYYYMMDD format (default: previous hour's date)",
        default=None
    )
    parser.add_argument(
        "--end-date",
        help="End date in YYYYMMDD format (default: previous hour's date)",
        default=None
    )
    return parser


def print_cameras_for_pvz(pvz_id):
    # Function to print camera information by PVZ_ID
    print("=== Camera list for PVZ_ID {} ===".format(pvz_id))
    pvz_cameras = CAMERAS_BY_PVZ.get(pvz_id, {})
    
    if not pvz_cameras:
        print("No cameras found for PVZ_ID {}".format(pvz_id))
        return
        
    for zone, cameras in pvz_cameras.items():
        print("\nZone: {}".format(zone))
        for i, camera in enumerate(cameras, 1):
            print("  Camera {}:".format(i))
            print("    ID: {}".format(camera['id']))
            print("    UID: {}".format(camera['uid']))
            print("    Location: {}".format(camera['локация']))


def main():
    # Parse command-line arguments
    parser = setup_argument_parser()
    args = parser.parse_args()
    
    # Determine the start and end dates and hours
    if args.start_date and args.end_date:
        start_date_str = args.start_date
        end_date_str = args.end_date
        # For custom date ranges, use the default hour range (9-21)
        start_hour = 9
        end_hour = 21
        
        print("=== Parameters from base_config.py ===")
        print("PVZ_ID: {}".format(PVZ_ID))
        print("ENV_MODE: {}".format(ENV_MODE))
        print()
        print("PATH_CONFIG:")
        for key, value in PATH_CONFIG.items():
            print("  {}: {}".format(key, value))
        print()
        print("Other directory parameters:")
        print("  BASE_DIR: {}".format(BASE_DIR))
        print("  LOGS_DIR: {}".format(LOGS_DIR))
        print("  SCHEDULER_RUNNER_DIR: {}".format(SCHEDULER_RUNNER_DIR))
        print("  TASKS_DIR: {}".format(TASKS_DIR))
        print()
        print("=== Parameters from cameras_paths.py ===")
        print("CAMERAS_LOCAL: {}".format(CAMERAS_LOCAL))
        print("CAMERAS_NETWORK: {}".format(CAMERAS_NETWORK))
        print("TELEGRAM_TOKEN: {}".format(TELEGRAM_TOKEN))
        print("TELEGRAM_CHAT_ID: {}".format(TELEGRAM_CHAT_ID))
        print()
        print("CAMERAS_PATHS:")
        for key, value in CAMERAS_PATHS.items():
            print("  {}: {}".format(key, value))
        print()
        print_cameras_for_pvz(PVZ_ID)
        
        # Get camera configurations for the PVZ
        pvz_cameras = CAMERAS_BY_PVZ.get(PVZ_ID, {})
        
        print("\n=== Creating JSON file with camera configurations ===")
        
        # Create a temporary JSON file with camera configurations
        json_file_path = project_root / "tests" / "TestEnvironment" / f"camera_config_pvz_{PVZ_ID}.json"
        
        try:
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(pvz_cameras, f, ensure_ascii=False, indent=2)
            print(f"Camera configurations saved to: {json_file_path}")
        except Exception as e:
            print(f"Error creating JSON file: {e}")
            return
        
        print("\n=== Calling standalone script for LOCAL camera structure (without PVZ ID in path) ===")
        
        # Prepare command for LOCAL camera structure with --no-pvz-id flag
        cmd_local = [
            sys.executable,
            str(project_root / "tests" / "TestEnvironment" / "create_detailed_camera_structure_standalone.py"),
            "--start-date", start_date_str,
            "--end-date", end_date_str, 
            "--start-hour", str(start_hour),
            "--end-hour", str(end_hour),
            "--output-dir", str(CAMERAS_LOCAL),
            "--pvz-id", str(PVZ_ID),
            "--cameras-config-file", str(json_file_path),
            "--force"  # Add force flag to avoid user input in subprocess
        ]
        
        print("Executing command for LOCAL structure: {}".format(" ".join(cmd_local)))
        
        # Execute the standalone script for local structure
        try:
            result = subprocess.run(cmd_local, check=True, capture_output=True, text=True)
            print("Local structure script executed successfully!")
            if result.stdout:
                print("Output: {}".format(result.stdout))
            if result.stderr:
                print("Errors: {}".format(result.stderr))
        except subprocess.CalledProcessError as e:
            print("Error executing standalone script for LOCAL structure: {}".format(e))
            print("Return code: {}".format(e.returncode))
            print("Output: {}".format(e.stdout))
            print("Error: {}".format(e.stderr))
        
        print("\n=== Creating directory structure for NETWORK camera structure (with PVZ ID in path) ===")
        
        # Create directory for network structure (CAMERAS_NETWORK is already includes PVZ_ID)
        # The path is structured as O_cameras\{PVZ_ID}, so we need to use parent directory
        network_output_dir = Path(CAMERAS_NETWORK)
        network_output_dir.parent.mkdir(parents=True, exist_ok=True)
        
        # Prepare command for NETWORK camera structure with --include-pvz-id flag
        cmd_network = [
            sys.executable,
            str(project_root / "tests" / "TestEnvironment" / "create_detailed_camera_structure_standalone.py"),
            "--start-date", start_date_str,
            "--end-date", end_date_str, 
            "--start-hour", str(start_hour),
            "--end-hour", str(end_hour),
            "--output-dir", str(network_output_dir.parent),
            "--pvz-id", str(PVZ_ID),
            "--include-pvz-id",  # Include PVZ ID in path for network structure
            "--cameras-config-file", str(json_file_path),
            "--force"  # Add force flag to avoid user input in subprocess
        ]
        
        print("Executing command for NETWORK structure: {}".format(" ".join(cmd_network)))
        
        # Execute the standalone script for network structure
        try:
            result = subprocess.run(cmd_network, check=True, capture_output=True, text=True)
            print("Network structure script executed successfully!")
            if result.stdout:
                print("Output: {}".format(result.stdout))
            if result.stderr:
                print("Errors: {}".format(result.stderr))
        except subprocess.CalledProcessError as e:
            print("Error executing standalone script for NETWORK structure: {}".format(e))
            print("Return code: {}".format(e.returncode))
            print("Output: {}".format(e.stdout))
            print("Error: {}".format(e.stderr))
        finally:
            # Clean up the temporary JSON file after execution
            try:
                json_file_path.unlink()
                print(f"\nTemporary JSON file {json_file_path} has been removed.")
            except Exception as e:
                print(f"Warning: Could not remove temporary JSON file {json_file_path}: {e}")

        print(f"\nCamera structure generation completed for both local ({CAMERAS_LOCAL}) and network ({CAMERAS_NETWORK}) directories.")
    elif args.start_date or args.end_date:
        # If only one date is provided, raise an error
        print("Error: Both --start-date and --end-date must be provided together.")
        return
    else:
        # Default behavior - using the previous hour, only for local directory
        date_str, hour = get_default_datetime()
        start_date_str = date_str
        end_date_str = date_str
        start_hour = hour
        end_hour = hour
        print(f"Using default date and hour based on previous hour: {date_str} at {hour:02d}:00")
        
        print("=== Parameters from base_config.py ===")
        print("PVZ_ID: {}".format(PVZ_ID))
        print("ENV_MODE: {}".format(ENV_MODE))
        print()
        print("PATH_CONFIG:")
        for key, value in PATH_CONFIG.items():
            print("  {}: {}".format(key, value))
        print()
        print("Other directory parameters:")
        print("  BASE_DIR: {}".format(BASE_DIR))
        print("  LOGS_DIR: {}".format(LOGS_DIR))
        print("  SCHEDULER_RUNNER_DIR: {}".format(SCHEDULER_RUNNER_DIR))
        print("  TASKS_DIR: {}".format(TASKS_DIR))
        print()
        print("=== Parameters from cameras_paths.py ===")
        print("CAMERAS_LOCAL: {}".format(CAMERAS_LOCAL))
        print("CAMERAS_NETWORK: {}".format(CAMERAS_NETWORK))
        print("TELEGRAM_TOKEN: {}".format(TELEGRAM_TOKEN))
        print("TELEGRAM_CHAT_ID: {}".format(TELEGRAM_CHAT_ID))
        print()
        print("CAMERAS_PATHS:")
        for key, value in CAMERAS_PATHS.items():
            print("  {}: {}".format(key, value))
        print()
        print_cameras_for_pvz(PVZ_ID)
        
        # Get camera configurations for the PVZ
        pvz_cameras = CAMERAS_BY_PVZ.get(PVZ_ID, {})
        
        print("\n=== Creating JSON file with camera configurations ===")
        
        # Create a temporary JSON file with camera configurations
        json_file_path = project_root / "tests" / "TestEnvironment" / f"camera_config_pvz_{PVZ_ID}.json"
        
        try:
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(pvz_cameras, f, ensure_ascii=False, indent=2)
            print(f"Camera configurations saved to: {json_file_path}")
        except Exception as e:
            print(f"Error creating JSON file: {e}")
            return
        
        print("\n=== Calling standalone script for LOCAL camera structure (without PVZ ID in path) ===")
        
        # Prepare command for LOCAL camera structure with --no-pvz-id flag
        cmd_local = [
            sys.executable,
            str(project_root / "tests" / "TestEnvironment" / "create_detailed_camera_structure_standalone.py"),
            "--start-date", start_date_str,
            "--end-date", end_date_str, 
            "--start-hour", str(start_hour),
            "--end-hour", str(end_hour),
            "--output-dir", str(CAMERAS_LOCAL),
            "--pvz-id", str(PVZ_ID),
            "--cameras-config-file", str(json_file_path),
            "--force"  # Add force flag to avoid user input in subprocess
        ]
        
        print("Executing command for LOCAL structure: {}".format(" ".join(cmd_local)))
        
        # Execute the standalone script for local structure
        try:
            result = subprocess.run(cmd_local, check=True, capture_output=True, text=True)
            print("Local structure script executed successfully!")
            if result.stdout:
                print("Output: {}".format(result.stdout))
            if result.stderr:
                print("Errors: {}".format(result.stderr))
        except subprocess.CalledProcessError as e:
            print("Error executing standalone script for LOCAL structure: {}".format(e))
            print("Return code: {}".format(e.returncode))
            print("Output: {}".format(e.stdout))
            print("Error: {}".format(e.stderr))
        finally:
            # Clean up the temporary JSON file after execution
            try:
                json_file_path.unlink()
                print(f"\nTemporary JSON file {json_file_path} has been removed.")
            except Exception as e:
                print(f"Warning: Could not remove temporary JSON file {json_file_path}: {e}")

        print(f"\nCamera structure generation completed for local directory ({CAMERAS_LOCAL}).")


if __name__ == "__main__":
    main()
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Generate test camera structures using cameras configs and path settings."""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.base_config import ENV_MODE, PVZ_ID  # noqa: E402
from scheduler_runner.tasks.cameras.config.cameras_list import CAMERAS_BY_PVZ  # noqa: E402
from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_NETWORK, CAMERAS_PATHS  # noqa: E402


def get_default_datetime():
    now = datetime.now() - timedelta(hours=1)
    return now.strftime("%Y%m%d"), now.hour


def setup_argument_parser():
    parser = argparse.ArgumentParser(
        description="Run camera structure generator. If no dates are set, generates previous hour for local roots only."
    )
    parser.add_argument("--start-date", default=None, help="Start date YYYYMMDD")
    parser.add_argument("--end-date", default=None, help="End date YYYYMMDD")
    return parser


def print_cameras_for_pvz(pvz_id):
    print(f"=== Camera list for PVZ_ID {pvz_id} ===")
    pvz_cameras = CAMERAS_BY_PVZ.get(pvz_id, {})
    if not pvz_cameras:
        print(f"No cameras found for PVZ_ID {pvz_id}")
        return
    for zone, cameras in pvz_cameras.items():
        print(f"\nZone: {zone}")
        for i, camera in enumerate(cameras, 1):
            print(f"  Camera {i}: ID={camera['id']}, UID={camera['uid']}, root_key={camera.get('root_key', 'default')}")


def build_local_camera_groups(pvz_cameras: dict, local_roots: dict) -> dict:
    """Split camera config by root_key while preserving zone structure."""
    groups = {key: {} for key in local_roots.keys()}
    default_key = "default" if "default" in local_roots else next(iter(local_roots.keys()))

    for zone, cameras in pvz_cameras.items():
        for camera in cameras:
            root_key = camera.get("root_key", default_key)
            if root_key not in groups:
                root_key = default_key
            groups.setdefault(root_key, {}).setdefault(zone, []).append(camera)

    return {key: value for key, value in groups.items() if value}


def run_standalone(start_date, end_date, start_hour, end_hour, output_dir, pvz_id, cameras_config, include_pvz_id):
    json_file_path = project_root / "tests" / "TestEnvironment" / f"camera_config_pvz_{pvz_id}_{'network' if include_pvz_id else output_dir.name}.json"

    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump(cameras_config, f, ensure_ascii=False, indent=2)

    cmd = [
        sys.executable,
        str(project_root / "tests" / "TestEnvironment" / "create_detailed_camera_structure_standalone.py"),
        "--start-date", start_date,
        "--end-date", end_date,
        "--start-hour", str(start_hour),
        "--end-hour", str(end_hour),
        "--output-dir", str(output_dir),
        "--pvz-id", str(pvz_id),
        "--cameras-config-file", str(json_file_path),
        "--force",
    ]
    if include_pvz_id:
        cmd.append("--include-pvz-id")

    print("Executing:", " ".join(cmd))
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)
    finally:
        try:
            json_file_path.unlink()
        except Exception as e:
            print(f"Warning: could not remove temp json {json_file_path}: {e}")


def main():
    args = setup_argument_parser().parse_args()

    pvz_cameras = CAMERAS_BY_PVZ.get(PVZ_ID, {})
    local_roots = CAMERAS_PATHS.get("LOCAL_ROOTS", {"default": CAMERAS_PATHS["CAMERAS_LOCAL"]})

    print("=== Cameras test generation params ===")
    print(f"PVZ_ID: {PVZ_ID}")
    print(f"ENV_MODE: {ENV_MODE}")
    print(f"LOCAL_ROOTS: {local_roots}")
    print(f"CAMERAS_NETWORK: {CAMERAS_NETWORK}")
    print_cameras_for_pvz(PVZ_ID)

    if not pvz_cameras:
        print("No cameras found for current PVZ, nothing to generate.")
        return

    if args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
        start_hour = 9
        end_hour = 21

        local_groups = build_local_camera_groups(pvz_cameras, local_roots)
        for root_key, cameras_subset in local_groups.items():
            output_dir = Path(local_roots[root_key])
            print(f"\n=== LOCAL generation for root_key={root_key}, output={output_dir} ===")
            run_standalone(start_date, end_date, start_hour, end_hour, output_dir, PVZ_ID, cameras_subset, include_pvz_id=False)

        print("\n=== NETWORK generation ===")
        network_output_parent = Path(CAMERAS_NETWORK).parent
        network_output_parent.mkdir(parents=True, exist_ok=True)
        run_standalone(start_date, end_date, start_hour, end_hour, network_output_parent, PVZ_ID, pvz_cameras, include_pvz_id=True)
        print("\nCamera structure generation completed for local roots and network.")

    elif args.start_date or args.end_date:
        print("Error: both --start-date and --end-date must be provided together")
        return

    else:
        date_str, hour = get_default_datetime()
        local_groups = build_local_camera_groups(pvz_cameras, local_roots)
        print(f"Using default previous hour: {date_str} {hour:02d}:00")
        for root_key, cameras_subset in local_groups.items():
            output_dir = Path(local_roots[root_key])
            print(f"\n=== LOCAL generation for root_key={root_key}, output={output_dir} ===")
            run_standalone(date_str, date_str, hour, hour, output_dir, PVZ_ID, cameras_subset, include_pvz_id=False)
        print("\nCamera structure generation completed for local roots.")


if __name__ == "__main__":
    main()

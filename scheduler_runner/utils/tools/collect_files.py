from pathlib import Path
import fnmatch

def should_exclude(path: Path, exclude_patterns: list) -> bool:
    """
    Checks if a file or directory should be excluded based on the exclusion patterns.
    """
    for pattern in exclude_patterns:
        if fnmatch.fnmatch(path.name, pattern):
            return True
    return False

def collect_files_from_directory(directory: Path, exclude_patterns: list, root_dir: Path, outfile):
    """
    Recursively collects files from the specified directory, excluding those matching the exclusion patterns.
    """
    for item in directory.iterdir():
        if item.is_dir():
            if not should_exclude(item, exclude_patterns):
                collect_files_from_directory(item, exclude_patterns, root_dir, outfile)
        elif item.is_file() and not should_exclude(item, exclude_patterns):
            relative_path = item.relative_to(root_dir)
            with open(item, 'r', encoding='utf-8') as infile:
                content = infile.read()
                outfile.write(f"# Содержимое файла: {relative_path}\n")
                outfile.write(content)
                outfile.write("\n\n")

def main():
    # Get the absolute path to the script's directory
    script_dir = Path(__file__).parent

    # Assume the root directory of the project is two levels up
    root_dir = script_dir.parent.parent.parent

    # List of directories to collect code from (relative to the root directory)
    directories_to_include = [
        "scheduler_runner/tasks/cameras",
    ]

    # List of individual files to collect (relative to the root directory)
    files_to_include = [
        "scheduler_runner/runner.py",
        "scheduler_runner/schedule_config.py",
    ]

    # List of patterns for excluding files and directories
    exclude_patterns = [
        "__pycache__",
        "*.pyc",
        "*.pyo",
        "*.pyd",
        ".pytest_cache",
        "venv",
        "ENV",
        "env",
        ".venv",
        "*.log",
        "logs",
        ".env",
        "*.env",
        ".vscode",
        ".idea",
        "*.sublime-project",
        "*.sublime-workspace",
        "*.egg-info",
        "dist",
        "build",
        "__init__.py",
        "README.md",
    ]

    # Path to the output file (in the same directory as the script)
    output_file = script_dir / "PythonCode.txt"

    with open(output_file, 'w', encoding='utf-8') as outfile:
        # Collect code from directories
        for dir_rel_path in directories_to_include:
            dir_path = root_dir / dir_rel_path
            if dir_path.exists() and dir_path.is_dir():
                collect_files_from_directory(dir_path, exclude_patterns, root_dir, outfile)
            else:
                print(f"Directory {dir_path} does not exist or is not a directory.")

        # Collect code from individual files
        for file_rel_path in files_to_include:
            file_path = root_dir / file_rel_path
            if file_path.exists() and file_path.is_file():
                relative_path = file_path.relative_to(root_dir)
                with open(file_path, 'r', encoding='utf-8') as infile:
                    content = infile.read()
                    outfile.write(f"# Содержимое файла: {relative_path}\n")
                    outfile.write(content)
                    outfile.write("\n\n")
            else:
                print(f"File {file_path} does not exist or is not a file.")

if __name__ == "__main__":
    main()
''' Move all files to seperate folders so they can be uploaded to s3 later '''

import os
import shutil
import configparser
from dotenv import load_dotenv
load_dotenv()

original_path = os.getenv("original_path")
local_path = os.getenv('local_path')
destination1 = os.getenv('local_path1')
destination2 = os.getenv('local_path2')
archive_path = os.getenv('archive_path')

def move_files_by_pattern(local_path, destination1, destination2):
    """
    Moves files from `local_path` to `destination1` if their names contain '',
    or to `destination2` if their names contain 'abc'.
    
    Args:
        local_path (str): Source directory containing files.
        destination1 (str): Target directory for files containing 'desired pattern'.
        destination2 (str): Target directory for files containing 'desired pattern'.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Source path does not exist: {local_path}")
    if not os.path.exists(destination1):
        os.makedirs(destination1)  # Create destination1 directory if it doesn't exist
    if not os.path.exists(destination2):
        os.makedirs(destination2)  # Create destination2 directory if it doesn't exist

    # List all files in the source directory
    for file_name in os.listdir(original_path):
        file_path = os.path.join(original_path, file_name)

        # Ensure it's a file
        if os.path.isfile(file_path):
            if "permit-imputations" in file_name:
                # Move to destination1
                target_path = os.path.join(destination1, file_name)
                shutil.move(file_path, target_path)
                print(f"Moved to destination1: {file_name}")
            elif "issued-dates" in file_name:
                # Move to destination2
                target_path = os.path.join(destination2, file_name)
                shutil.move(file_path, target_path)
                print(f"Moved to destination2: {file_name}")
            elif "reveal-gc-2024-" in file_name:
                # Move to archieve
                target_path = os.path.join(local_path, file_name)
                shutil.copy(file_path, target_path)
                print(f"Copied to local: {file_name}")
            else:
                print(f"Skipped: {file_name}")
        else:
            print(f"Skipping non-file: {file_name}")

    """
    Reads configuration from a config.ini file.
    
    Args:
        config_path (str): Path to the config.ini file.
    
    Returns:
        dict: A dictionary with `local_path`, `destination1`, and `destination2`.
    """
move_files_by_pattern(local_path, destination1, destination2)
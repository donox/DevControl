import os
import zipfile
import shutil
from datetime import datetime


def unzip_file(zip_filename, target_directory="/home/don/Documents/Temp/dev990/data/work_files"):
    """
    Unzip a file into a directory, moving existing directory contents to trash first

    Args:
        zip_filename: Path to the zip file
        target_directory: Directory to unzip contents into

    Returns:
        bool: True if successful, False if error occurred
    """
    try:
        # If directory exists, move its contents to trash
        if os.path.exists(target_directory):
            trash_dir = os.path.expanduser('~/.local/share/Trash/files')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            trash_name = f"{os.path.basename(target_directory)}_{timestamp}"
            trash_path = os.path.join(trash_dir, trash_name)

            # Move existing directory to trash
            shutil.move(target_directory, trash_path)

        # Create fresh directory
        os.makedirs(target_directory, exist_ok=True)

        # Unzip the file
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            zip_ref.extractall(target_directory)

        return True

    except Exception as e:
        print(f"Error unzipping {zip_filename}: {str(e)}")
        return False

# Example usage:
# success = unzip_file('downloaded_file.zip', '/path/to/extract')

def clean_directory(directory):
    """
    Delete files in directory that don't have '990PF' in their name

    Args:
        directory: Directory to clean

    Returns:
        tuple: (number of files deleted, number of files kept)
    """
    deleted = 0
    kept = 0

    try:
        # Iterate through files in directory
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)

            # Check if it's a file (not a directory)
            if os.path.isfile(filepath):
                if '990PF' not in filename:
                    os.remove(filepath)
                    deleted += 1
                else:
                    kept += 1

        return deleted, kept

    except Exception as e:
        print(f"Error cleaning directory {directory}: {str(e)}")
        return None, None

# Example usage:
# deleted, kept = clean_directory('/path/to/directory')
# print(f"Deleted {deleted} files, kept {kept} files")
import os
import zipfile
import shutil
from datetime import datetime


def unzip_driver(filename_generator, **kwargs):
    try:
        print(f"Received filename_generator: {type(filename_generator)}")
        for zip_filename in filename_generator:
            result = unzip_file(zip_filename, **kwargs)
            yield result
    except StopIteration:
        return None


def unzip_file(zip_filename, **kwargs):
    """
    Unzip a file into a structured directory.

    Args:
        zip_filename (str): Path to the zip file.
        directory (str): Target base directory to unzip contents into.
        delete_current (bool, optional): Whether to delete existing directory if it exists.

    Returns:
        bool: True if successful or directory already exists and processing is skipped, False if error occurred.
    """
    try:
        # Extract parameters
        target_directory = kwargs.get('directory')
        delete_current = kwargs.get('delete_current', False)

        if not target_directory:
            raise ValueError("The 'directory' parameter is required in kwargs.")

        # Get the base name for the subdirectory from the zip file name
        base_name = os.path.splitext(os.path.basename(zip_filename))[0]
        subdirectory = os.path.join(target_directory, base_name)

        # Check if directory already exists
        if os.path.exists(subdirectory):
            if delete_current:
                # Delete the existing directory
                shutil.rmtree(subdirectory)
                print(f"Existing directory '{subdirectory}' deleted as per 'delete_current' parameter.")
            else:
                # Log and return True if the directory exists and processing is skipped
                print(f"Directory '{subdirectory}' already exists. Skipping unzipping.")
                return subdirectory

        # Create the base and subdirectories
        raw_directory = os.path.join(subdirectory, 'raw')
        os.makedirs(raw_directory, exist_ok=True)
        processed_directory = os.path.join(subdirectory, 'processed')
        os.makedirs(processed_directory, exist_ok=True)

        # Unzip the file into the raw subdirectory
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            zip_ref.extractall(raw_directory)

        print(f"Unzipped '{zip_filename}' into '{raw_directory}'.")
        return subdirectory

    except Exception as e:
        print(f"Error unzipping '{zip_filename}': {e}")
        return None



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
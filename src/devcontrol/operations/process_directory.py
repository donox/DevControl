import json
import logging
from exceptions.app_exceptions import InvalidInputError
import os

logger = logging.getLogger("application")

def process_directory(input_data):
    """Generic handler for processing directories."""
    try:
        if is_valid_directory(input_data):
            raise InvalidInputError("Input data must be a directory.")
        output = {}
        for filename in os.listdir(input_data):
            file_path = os.path.join(input_data, filename)
            if os.path.isfile(file_path):
                with open(file_path, 'r') as file:
                    content = json.load(file)
                # Perform a transformation (example: add metadata)
                output[filename] = {key: value * 2 if isinstance(value, (int, float)) else value for key, value in content.items()}
            # Handle nested directory
        return output
    except Exception as e:
        logger.error(f"Error in process_directory: {e}")
        raise

def is_valid_directory(path):
    try:
        os.listdir(path)
        return True
    except (FileNotFoundError, NotADirectoryError, PermissionError):
        return False

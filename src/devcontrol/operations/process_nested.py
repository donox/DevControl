import logging
from exceptions.app_exceptions import InvalidInputError

logger = logging.getLogger("application")

def process_nested(input_data):
    """Generic handler for processing nested structures."""
    if isinstance(input_data, dict):
        return {key: process_nested(value) for key, value in input_data.items()}
    elif isinstance(input_data, list):
        return [process_nested(item) for item in input_data]
    elif isinstance(input_data, (int, float)):
        return input_data * 2  # Example transformation
    else:
        return input_data

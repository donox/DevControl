import logging
from exceptions.app_exceptions import InvalidInputError

logger = logging.getLogger("application")


def process_dict(input_data):
    """Generic handler for processing dictionaries."""
    try:
        if not isinstance(input_data, dict):
            raise InvalidInputError("Input data must be a dictionary.")
        return {key: value * 2 if isinstance(value, (int, float)) else value for key, value in input_data.items()}
    except Exception as e:
        logger.error(f"Error in process_dict: {e}")
        raise

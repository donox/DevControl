import logging
from exceptions.app_exceptions import InvalidInputError

logger = logging.getLogger("application")

def process_list(input_data):
    """Generic handler for processing lists."""
    try:
        if not isinstance(input_data, list):
            raise InvalidInputError("Input data must be a dictionary.")
        return [item * 2 if isinstance(item, (int, float)) else item for item in input_data]
    except Exception as e:
        logger.error(f"Error in process_list: {e}")
        raise


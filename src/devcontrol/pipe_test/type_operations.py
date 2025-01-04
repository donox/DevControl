# pipe_test/type_operations.py
import logging
from datetime import datetime
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def test_numeric_types(data):
    """Handle different numeric types"""
    try:
        # Integer
        if isinstance(data, int):
            return data * 2
        # Float
        elif isinstance(data, float):
            return round(data * 1.5, 2)
        # Convert string to number
        elif isinstance(data, str) and data.replace('.','',1).isdigit():
            num = float(data)
            return round(num * 1.5, 2)
        else:
            return 0
    except Exception as e:
        logger.error(f"Error in numeric handling: {e}")
        return 0


def test_string_types(data):
    """Handle different string formats"""
    try:
        if isinstance(data, str):
            return data.upper()
        return str(data).upper()
    except Exception as e:
        logger.error(f"Error in string handling: {e}")
        return "ERROR"


def test_list_types(data):
    """Handle different list-like types"""
    try:
        # Regular list
        if isinstance(data, list):
            return [f"item_{i}" for i in data]
        # Tuple
        elif isinstance(data, tuple):
            return tuple(f"item_{i}" for i in data)
        # Set
        elif isinstance(data, set):
            return {f"item_{i}" for i in data}
        else:
            return []
    except Exception as e:
        logger.error(f"Error in list handling: {e}")
        return []


def test_dict_types(data):
    """Handle different dictionary formats"""
    try:
        if isinstance(data, dict):
            return {k: f"value_{v}" for k, v in data.items()}
        return {"error": "invalid_input"}
    except Exception as e:
        logger.error(f"Error in dict handling: {e}")
        return {"error": str(e)}


def test_datetime_types(data):
    """Handle different datetime formats"""
    try:
        if isinstance(data, str):
            dt = datetime.fromisoformat(data.replace('Z', '+00:00'))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(data, datetime):
            return data.strftime("%Y-%m-%d %H:%M:%S")
        return "invalid_date"
    except Exception as e:
        logger.error(f"Error in datetime handling: {e}")
        return "invalid_date"


# In pipe_test/type_operations.py
def test_array_types(data):
    """Handle different array-like types"""
    try:
        if isinstance(data, (list, tuple)):
            arr = np.array(data)
            result = (arr * 2).tolist()  # Convert to list for JSON serialization
            return result
        elif isinstance(data, np.ndarray):
            return (data * 2).tolist()  # Convert to list for JSON serialization
        return []
    except Exception as e:
        logger.error(f"Error in array handling: {e}")
        return []


def test_dataframe_types(data):
    """Handle different dataframe formats"""
    try:
        if isinstance(data, dict):
            df = pd.DataFrame.from_dict(data)
            return df.to_dict('records')
        elif isinstance(data, list):
            df = pd.DataFrame(data)
            return df.to_dict('records')
        return []
    except Exception as e:
        logger.error(f"Error in dataframe handling: {e}")
        return []

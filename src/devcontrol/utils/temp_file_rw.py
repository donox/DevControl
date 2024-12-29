import json
from utils.json_enhanced import NumpyEncoder
import pandas as pd
import os
import tempfile

def write_to_temp_file(data, suffix=".json"):
    """
    Writes data to a temporary file and returns the file path.

    Args:
        data (any): Data to write to the file.
        suffix (str): File extension.

    Returns:
        str: Path to the temporary file.
    """
    encoder = NumpyEncoder()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    if suffix == ".json":
        with open(temp_file.name, 'w') as f:
            json_str = encoder.encode(data)
            f.write(json_str)
    elif suffix == ".csv":
        data.to_csv(temp_file.name, index=False)
    temp_file.close()
    return temp_file.name

def read_from_temp_file(file_path):
    """
    Reads data from a temporary file based on its extension.

    Args:
        file_path (str): Path to the temporary file.

    Returns:
        any: Data read from the file.
    """
    if file_path.endswith(".json"):
        with open(file_path, 'r') as f:
            return json.load(f)
    elif file_path.endswith(".csv"):
        return pd.read_csv(file_path)

def delete_temp_file(file_path):
    os.remove(file_path)
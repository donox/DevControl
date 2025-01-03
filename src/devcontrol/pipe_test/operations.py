# pipe_test/operations.py

def basic_transform(data):
    """Basic transformation that uppercases string data"""
    return str(data).upper()


def file_transform(data):
    """Transform data from a file"""
    if isinstance(data, dict):
        return {k: str(v).upper() for k, v in data.items()}
    return data


def data_transform(data):
    """Transform data for database storage"""
    if isinstance(data, (list, tuple)):
        return [str(x).upper() for x in data]
    return str(data).upper()


def string_transform(data):
    """Transform string data"""
    return f"PROCESSED_{data}"


def no_transform(data):
    """Function that returns data unchanged"""
    return data


def increment_number(data):
    """Adds 1 to input number"""
    try:
        return int(data) + 1
    except (ValueError, TypeError):
        return 0


def multiply_number(data):
    """Multiplies input by 2"""
    try:
        return int(data) * 2
    except (ValueError, TypeError):
        return 0


def transform_string(data):
    """Adds prefix and suffix to string"""
    return f"PREFIX_{str(data)}_SUFFIX"


def transform_list(data):
    """Adds index prefix to each list item"""
    if not isinstance(data, list):
        return ["INVALID_INPUT"]
    return [f"item_{i}_{x}" for i, x in enumerate(data)]


def transform_dict(data):
    """Adds key prefix to each dict value"""
    if not isinstance(data, dict):
        return {"error": "invalid_input"}
    return {k: f"key_{k}_{v}" for k, v in data.items()}


def identity_function(data):
    """Returns input unchanged"""
    return data
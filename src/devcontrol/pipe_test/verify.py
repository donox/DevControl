# pipe_test/verify.py
import numpy as np


def verify_number_test(output_data):
    """Verify number transformation results"""
    # For increment_number(5) -> 6 -> multiply_number(6) -> 12
    expected = 12
    return output_data == expected


def verify_string_test(output_data):
    """Verify string transformation results"""
    expected = "PREFIX_test_string_SUFFIX"
    return output_data == expected


def verify_list_test(output_data):
    """Verify list transformation results"""
    expected = ["item_0_a", "item_1_b", "item_2_c"]
    return output_data == expected


def verify_dict_test(output_data):
    """Verify dictionary transformation results"""
    expected = {
        "key1": "key_key1_value1",
        "key2": "key_key2_value2"
    }
    return output_data == expected


def verify_identity_test(output_data, input_data):
    """Verify identity transformation results"""
    return output_data == input_data


def verify_numeric_test(output_data):
    """Verify numeric transformation results"""
    int_test = output_data[0] == 84  # 42 * 2
    float_test = abs(output_data[1] - 4.71) < 0.01  # 3.14 * 1.5
    return int_test and float_test


def verify_string_test(output_data):
    """Verify string transformation results"""
    # Check for either type_operations format (uppercase)
    # or regular operations format (prefix/suffix)
    return (output_data == "TEST STRING" or
            output_data == "PREFIX_test_string_SUFFIX")


def verify_collection_test(output_data):
    """Verify collection transformation results"""
    list_test = output_data[0] == ["item_1", "item_2", "item_3"]
    dict_test = output_data[1] == {"a": "value_1", "b": "value_2"}
    return list_test and dict_test


def verify_datetime_test(output_data):
    """Verify datetime transformation results"""
    return output_data == "2024-01-03 12:00:00"


# In pipe_test/verify.py
def verify_array_test(output_data):
    """Verify array transformation results"""
    expected = [2, 4, 6, 8, 10]
    return output_data == expected


def verify_dataframe_test(output_data):
    """Verify dataframe transformation results"""
    expected = [
        {"col1": 1, "col2": "a"},
        {"col1": 2, "col2": "b"},
        {"col1": 3, "col2": "c"}
    ]
    return output_data == expected


# verify generators
def verify_simple_generator_test(output_data):
    """Verify simple generator results"""
    expected = ["URL1", "URL2", "URL3", "URL4", "URL5"]
    return output_data == expected


def verify_filtered_generator_test(output_data):
    """Verify filtered generator results"""
    # Should only have processed even numbers
    expected = [4, 8, 12, 16, 20]  # Even numbers * 2
    return output_data == expected


def verify_chained_generator_test(output_data):
    """Verify chained generator results"""
    # Numbers > 5 are processed first (* 2), then converted to strings
    expected = ["12", "16", "14", "18"]  # (6, 8, 7, 9) * 2 -> string
    return output_data == expected


def verify_limited_generator_test(output_data):
    """Verify limited generator results"""
    expected = ["ITEM1", "ITEM2", "ITEM3"]  # Only first 3 items
    return output_data == expected

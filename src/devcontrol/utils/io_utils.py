import importlib
import os
import json

def load_function(name, function_name, pkg='operations'):
    """Dynamically loads a function from a module."""
    module_name = pkg + '.' + name
    module = importlib.import_module(module_name)
    return getattr(module, function_name)

def read_json(file_path):
    """Reads JSON data from a file."""
    with open(file_path, 'r') as file:
        return json.load(file)

def write_json(data, file_path):
    """Writes JSON data to a file."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)


def run_printer():
    inputfile = "/tmp/tmp1zi46ztv.json"
    outputfile = "/home/don/Documents/Temp/WW990/structure/bbox.txt"
    print(f"running")
    json_to_text_formatted(inputfile, outputfile)


def json_to_text_formatted(json_file_path, output_text_path):
    try:
        # Read the JSON file
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)

        # Write to text file with pretty formatting
        with open(output_text_path, 'w') as text_file:
            text_file.write(json.dumps(data, indent=4))

        return True
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

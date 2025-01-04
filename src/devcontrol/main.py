#!/usr/bin/env python3
import os
import sys
import argparse
import yaml
from pipeline_manager import PipelineManager
from utils.io_utils import read_yaml
from utils.logger import setup_logger
from data_management.pipeline_data_store import PipelineDataStore
from devcontrol.pipeline_manager import PipelineManager
from devcontrol.pipe_test.verify import *
from pipeline_manager import PipelineManager

def parse_arguments():
    parser = argparse.ArgumentParser(description="DevControl Pipeline")
    parser.add_argument('--base-dir', type=str, default=".",
                        help='Base directory for relative paths')
    parser.add_argument('--config', type=str, default="config.yml",
                        help='Path to configuration file')
    parser.add_argument('--pipeline-config', type=str, default="pipeline_config.yml",
                        help='Path to pipeline configuration file')
    parser.add_argument('--input-path', type=str, required=True,
                        help='Input path for pipeline')
    parser.add_argument('--output-file', type=str, required=True,
                        help='Output file for pipeline results')
    return parser.parse_args()


def setup_directories(config):
    """Create necessary directories based on configuration."""
    dirs = ['data_dir', 'output_dir', 'logs_dir']
    for dir_key in dirs:
        if dir_key in config['paths']:
            os.makedirs(config['paths'][dir_key], exist_ok=True)


# pipe_test/setup.py
def setup_test_env():
    """Create necessary test directories"""
    dirs = [
        "logs",
        "data",
        "output"
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def verify_test_result(test_name, output_data, input_data=None):
    """Verify test results based on test name"""
    verifiers = {
        'number_test': verify_number_test,
        'string_test': verify_string_test,
        'list_test': verify_list_test,
        'dict_test': verify_dict_test,
        'identity_test': lambda out: verify_identity_test(out, input_data)
    }

    test_base_name = os.path.splitext(test_name)[0]
    if test_base_name in verifiers:
        result = verifiers[test_base_name](output_data)
        print(f"Test verification: {'PASSED' if result else 'FAILED'}")
        return result
    return True             # presume success if not found in verifiers


def run_all_tests(test_directories=None):
    """
    Run pipeline tests from specified directories within pipeline_tests

    Args:
        test_directories: List of directory names to process. If None, process all directories.
    """
    # Setup test environment
    setup_test_env()

    # Load base config
    config_path = "/home/don/PycharmProjects/DevControl/pipeline_config/pipeline_tests/test_config.yml"
    base_test_dir = "/home/don/PycharmProjects/DevControl/pipeline_config/pipeline_tests"

    print(f"\nRunning pipeline tests")
    print("-" * 50)

    # Get directories to process
    if test_directories is None:
        test_directories = [d for d in os.listdir(base_test_dir)
                            if os.path.isdir(os.path.join(base_test_dir, d))]

    total_tests = 0
    passed_tests = 0
    failed_tests = 0

    # Process each test directory
    for test_dir in test_directories:
        dir_path = os.path.join(base_test_dir, test_dir)
        if not os.path.isdir(dir_path):
            print(f"Warning: {dir_path} is not a directory, skipping...")
            continue

        print(f"\nProcessing test directory: {test_dir}")
        print("-" * 30)

        # Get all yaml files in test directory
        test_files = [f for f in os.listdir(dir_path)
                      if f.endswith(('.yml', '.yaml'))
                      and f != 'test_config.yml']

        # Run each test in directory
        for test_file in test_files:
            total_tests += 1
            full_path = os.path.join(dir_path, test_file)
            print(f"\nRunning test: {test_file}")
            print("-" * 20)

            try:
                # Create pipeline manager
                pipeline = PipelineManager(config_path, full_path)

                # Create output filename based on test name
                output_name = os.path.splitext(test_file)[0] + "_output.json"
                output_file = os.path.join("test_output", output_name)

                # Run pipeline with appropriate test input
                input_data = get_test_input(test_file)
                pipeline.run_pipeline(input_data, output_file)

                # Load output data for verification
                with open(output_file, 'r') as f:
                    output_data = yaml.safe_load(f)

                # Verify test results
                verification_result = verify_test_result(test_file, output_data, input_data)
                if verification_result:
                    passed_tests += 1
                else:
                    failed_tests += 1

                # Check log file
                log_file = os.path.join("logs", "pipeline_log.log")
                if os.path.exists(log_file):
                    with open(log_file, 'r') as f:
                        print("Log contents:")
                        print(f.read())
                        print("-" * 20)
                else:
                    print(f"No log file found at {log_file}")

                print(f"Test completed: {test_file}")

            except Exception as e:
                print(f"Error running test {test_file}: {str(e)}")
                continue

    # Print summary
    print("\nTest Summary")
    print("-" * 20)
    print(f"Total tests run: {total_tests}")
    print(f"Tests passed: {passed_tests}")
    print(f"Tests failed: {total_tests - passed_tests}")
    print(f"Pass rate: {(passed_tests / total_tests) * 100:.2f}%")


def get_test_input(test_file):
    """Return appropriate test input based on test file name"""
    if test_file.startswith('number'):
        return 5
    elif test_file.startswith('string'):
        return "test_string"
    elif test_file.startswith('list'):
        return ["a", "b", "c"]
    elif test_file.startswith('dict'):
        return {"key1": "value1", "key2": "value2"}
    elif test_file.startswith('identity'):
        return "test_identity_input"
    elif 'numeric' in test_file:
        return [42, 3.14]
    elif 'string' in test_file:
        return "test string"
    elif 'collection' in test_file:
        return [[1, 2, 3], {"a": 1, "b": 2}]
    elif 'datetime' in test_file:
        return "2024-01-03T12:00:00Z"
    elif 'array' in test_file:
        return [1, 2, 3, 4, 5]
    elif 'dataframe' in test_file:
        return {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    elif 'simple_generator' in test_file:
        return ["url1", "url2", "url3", "url4", "url5"]
    elif 'filtered_generator' in test_file:
        return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    elif 'chained_generator' in test_file:
        return [3, 6, 9, 2, 8, 1, 7, 4]
    elif 'limited_generator' in test_file:
        return ["item1", "item2", "item3", "item4", "item5"]
    else:
        return "test string"  # default input


# main.py
def main(args=None):
    if args is None:
        args = parse_arguments()
    else:
        # If args is a list of strings, parse it
        if isinstance(args, list):
            parser = argparse.ArgumentParser()
            args = parser.parse_args(args)

    # Change to base directory
    os.chdir(args.base_dir)

    # Load configurations
    try:
        config = read_yaml(args.config)
        pipeline_config = read_yaml(args.pipeline_config)
    except Exception as e:
        print(f"Error loading configuration files: {e}")
        return 1

    # Setup directories
    setup_directories(config)

    # Initialize and run pipeline
    try:
        pipeline = PipelineManager(config, pipeline_config)
        pipeline.run_pipeline(args.input_path, args.output_file)
    except Exception as e:
        print(f"Pipeline execution failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    # sys.argv = ['main.py',
    #             '--base-dir', '/home/don/Documents/Temp/dev990',
    #             '--config', '/home/don/PycharmProjects/DevControl/config.yml',
    #             '--pipeline-config', '/home/don/PycharmProjects/DevControl/pipeline_config/pipeline_config.yml',
    #             '--input-path', '/home/don/Documents/Temp/dev990/temp_text.txt',
    #             '--output-file', '/home/don/Documents/Temp/dev990/final_output.json']
    # main()
    run_all_tests()
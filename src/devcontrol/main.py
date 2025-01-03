#!/usr/bin/env python3
import os
import sys
import argparse
from pipeline_manager import PipelineManager
from utils.io_utils import read_yaml
from utils.logger import setup_logger
from data_management.pipeline_data_store import PipelineDataStore
from devcontrol.pipeline_manager import PipelineManager


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


def run_all_tests():
    # Setup test environment
    setup_test_env()

    # Load base config
    config_path = "/home/don/PycharmProjects/DevControl/pipeline_config/pipeline_tests/test_config.yml"
    test_dir = "/home/don/PycharmProjects/DevControl/pipeline_config/pipeline_tests/single_step"

    print(f"\nRunning all pipeline tests from {test_dir}")
    print("-" * 50)

    # Get all yaml files in test directory except test_config.yml
    test_files = [f for f in os.listdir(test_dir)
                  if f.endswith(('.yml', '.yaml'))
                  and f != 'test_config.yml']

    for test_file in test_files:
        full_path = os.path.join(test_dir, test_file)
        print(f"\nRunning test: {test_file}")
        print("-" * 30)

        try:
            # Create pipeline manager
            pipeline = PipelineManager(config_path, full_path)

            # Create output filename based on test name
            output_name = os.path.splitext(test_file)[0] + "_output.json"
            output_file = os.path.join("test_output", output_name)

            # Run pipeline with test input
            input_data = "test string"
            pipeline.run_pipeline(input_data, output_file)

            # Check log file
            log_file = os.path.join("logs", "pipeline_log.log")
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    print("Log contents:")
                    print(f.read())
                    print("-" * 30)
            else:
                print(f"No log file found at {log_file}")

            print(f"Test completed: {test_file}")

        except Exception as e:
            print(f"Error running test {test_file}: {str(e)}")
            continue


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
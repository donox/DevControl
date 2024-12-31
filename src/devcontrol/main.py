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


# pipeline_manager.py
# class PipelineManager:
#     def __init__(self, config, pipeline_config):
#         """Initialize PipelineManager with either file paths or config dictionaries
#
#         Args:
#             config: Either a path to config file or config dictionary
#             pipeline_config: Either a path to pipeline config file or pipeline config dictionary
#         """
#         try:
#             # Load config if it's a path, otherwise use it directly
#             self.config = read_yaml(config) if isinstance(config, (str, bytes, os.PathLike)) else config
#             self.pipeline_config = read_yaml(pipeline_config) if isinstance(pipeline_config, (
#             str, bytes, os.PathLike)) else pipeline_config
#
#             self.pipeline_log = os.path.join(".", self.config['paths']["logs_dir"], "pipeline_log.log")
#             self.logger = setup_logger("pipeline", self.pipeline_log)
#             self.data_store = PipelineDataStore(
#                 base_path=os.path.join(".", self.config['paths'].get("data_dir", "pipeline_data"))
#             )
#         except Exception as e:
#             print(f"Error initializing PipelineManager: {e}")
#             raise


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
    sys.argv = ['main.py',
                '--base-dir', '/home/don/Documents/Temp/dev990',
                '--config', '/home/don/PycharmProjects/DevControl/config.yml',
                '--pipeline-config', '/home/don/PycharmProjects/DevControl/pipeline_config/pipeline_config.yml',
                '--input-path', '/home/don/Documents/Temp/dev990/temp_text.txt',
                '--output-file', '/home/don/Documents/Temp/dev990/final_output.json']
    main()

# c_path = "/home/don/PycharmProjects/DevControl/config.cfg"
# config = load_config(config_path=c_path)  # working directory set for script and is already set.
# operational_data = load_operational_data()
# pipeline_config = operational_data["pipeline"]
# pipeline = PipelineManager(config, pipeline_config)
# infile = "/home/don/Documents/Temp/Dev990/temp_text.txt"
# outfile = os.path.join(".", config['paths']["output_dir"], "final_output.json")
# pipeline.run_pipeline(infile, outfile)

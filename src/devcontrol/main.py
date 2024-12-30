import json
import os
from utils.io_utils import read_json, write_json, load_function
from utils.logger import setup_logger
from pipeline_manager import PipelineManager
import configparser
from importlib import resources


def load_config(config_path='config.cfg', base_dir=None):
    if base_dir is None:
        base_dir = os.getcwd()  # fallback, but better to explicitly provide

    config = configparser.ConfigParser()
    x = config.read(config_path)

    # Convert relative paths to absolute
    for key in config['paths']:
        relative_path = config['paths'][key]
        config['paths'][key] = os.path.join(base_dir, relative_path)
    return config


def load_operational_data():
    """Load JSON configuration that defines operational behavior"""
    with resources.open_text('pipeline_config', 'pipeline_config.json') as f:
        return json.load(f)

# Paths for working directories and logs
# working_dir = '/home/don/Documents/Temp/WW990/structure/'
# logs_dir = os.path.join(working_dir, 'logs/')
# input_dir = os.path.join(working_dir, 'input/')
# intermediates_dir = os.path.join(working_dir, 'intermediates/')


if __name__ == "__main__":
    c_path = "/home/don/PycharmProjects/DevControl/config.cfg"
    config = load_config(config_path=c_path)  # working directory set for script and is already set.
    operational_data = load_operational_data()
    pipeline_config = operational_data["pipeline"][0]  # JSON seems to have extra list wrapper
    pipeline = PipelineManager(config, pipeline_config)
    infile = "/home/don/Documents/Temp/Dev990/temp_text.txt"
    outfile = os.path.join(".", config['paths']["output_dir"], "final_output.json")
    pipeline.run_pipeline(infile, outfile)

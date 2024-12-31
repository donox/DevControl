import os
import json
import yaml
from utils.logger import setup_logger
from utils.io_utils import read_json, write_json, load_function, read_yaml
from operations import process_list, process_dict, process_directory, process_nested
from data_management.pipeline_data_store import PipelineDataStore


class PipelineManager:
    def __init__(self, config, pipeline_config):
        """Initialize PipelineManager with either file paths or config dictionaries

        Args:
            config: Either a path to config file or config dictionary
            pipeline_config: Either a path to pipeline config file or pipeline config dictionary
        """
        try:
            # Load config if it's a path, otherwise use it directly
            self.config = read_yaml(config) if isinstance(config, (str, bytes, os.PathLike)) else config
            self.pipeline_config = read_yaml(pipeline_config) if isinstance(pipeline_config, (
            str, bytes, os.PathLike)) else pipeline_config

            self.pipeline_log = os.path.join(".", self.config['paths']["logs_dir"], "pipeline_log.log")
            self.logger = setup_logger("pipeline", self.pipeline_log)
            self.data_store = PipelineDataStore(
                base_path=os.path.join(".", self.config['paths'].get("data_dir", "pipeline_data")))
        except Exception as e:
            print(f"Error initializing PipelineManager: {e}")
            raise

    def _process_directory(self, step_config, input_path):
        """Processes a directory, including nested subdirectories."""
        step_name = step_config["step_name"]
        output_data = {}

        self.logger.info(f"{step_name}: Processing directory {input_path}.")
        for root, _, files in os.walk(input_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                if os.path.isfile(file_path):
                    self.logger.debug(f"Processing file {file_path}.")
                    file_input = read_json(file_path)
                    processed = self.execute_operation(step_config, file_input, os.path.relpath(file_path, input_path))
                    output_data[os.path.relpath(file_path, input_path)] = processed

        return output_data

    def _process_nested(self, step_config, input_data):
        """Handles processing dynamically based on the actual input data type."""
        step_name = step_config["step_name"]
        skip = step_config.get("skip_sequencing", False)
        explicit_input = step_config.get("explicit_input", None)
        self.logger.debug(f"Processing step '{step_name}' with input data structure: {type(input_data)}")

        try:
            # Handle explicit input file
            if explicit_input:
                self.logger.info(f"{step_name}: Using explicit input file {explicit_input}.")
                if not os.path.exists(explicit_input):
                    raise FileNotFoundError(f"Explicit input file {explicit_input} not found.")
                input_extension = os.path.splitext(explicit_input)[1].lower()
                if input_extension == ".json":
                    with open(explicit_input, "r") as file:
                        input_data = json.load(file)  # Adjust for JSON files
                else:
                    input_data = explicit_input  # Pass non-JSON files as paths

                # Bypass further processing if explicit input is non-JSON
                if input_extension != ".json":
                    self.logger.info(f"{step_name}: Skipping nested processing for non-JSON explicit input.")
                    return self.execute_operation(step_config, input_data, step_name)

            # Handle intermediate JSON files with references
            if isinstance(input_data, str) and os.path.isfile(input_data):
                self.logger.info(f"{step_name}: Processing intermediate file {input_data}.")
                # with open(input_data, "r") as file:
                #     intermediate_data = json.load(file)
                #
                # # Check if the JSON contains a reference to another file
                # if intermediate_data.get("indirect_reference", False):
                #     tmp_file_path = intermediate_data["filename"]
                #     self.logger.info(f"{step_name}: Found referenced file {tmp_file_path}.")
                #     if not os.path.exists(tmp_file_path):
                #         raise FileNotFoundError(f"Referenced file {tmp_file_path} not found.")
                #     with open(tmp_file_path, "r") as tmp_file:
                #         input_data = json.load(tmp_file)  # Adjust for file type as needed
                # else:
                #     input_data = intermediate_data  # No referenced file, use intermediate data directly

            if not skip:
                # Handle dictionary (nested structure)
                if isinstance(input_data, dict):
                    self.logger.info(f"{step_name}: Processing dictionary structure.")
                    return {
                        key: self._process_nested(step_config, value)
                        for key, value in input_data.items()
                    }

                # Handle list
                elif isinstance(input_data, list):
                    self.logger.info(f"{step_name}: Processing list of items.")
                    return [
                        self.execute_operation(step_config, item, f"{step_name}_item_{i}")
                        for i, item in enumerate(input_data)
                    ]

            # Handle terminal (non-iterable) values
            elif skip or isinstance(input_data, (int, float, str, bool, type(None))):
                self.logger.info(f"{step_name}: Processing terminal value of type {type(input_data)}.")
                return self.execute_operation(step_config, input_data, step_name)

            # Handle unsupported types
            else:
                self.logger.error(f"{step_name}: Unsupported input type {type(input_data)}.")
                raise ValueError(f"Unsupported input type: {type(input_data)}")

        except Exception as e:
            self.logger.error(f"Error in step '{step_name}': {e}")
            raise

    def execute_operation(self, step_config, previous_step_data, step_context):
        """Execute operation with simplified processing modes"""
        step_name = step_config["step_name"]
        input_config = step_config.get("input", {"mode": "previous"})

        # Handle input mode
        if input_config["mode"] == "storage":
            input_data = self.data_store.get_step_input(
                step_name=step_name,
                storage_config=input_config["storage"]
            )
        elif input_config["mode"] == "passthrough":
            return previous_step_data
        else:  # "previous"
            input_data = previous_step_data

        # Handle processing mode
        process_mode = step_config.get("process_mode", "nested")
        try:
            if process_mode == "none":
                output_data = input_data
            else:
                operation = load_function(step_config["module"], step_config["function"])
                if process_mode == "nested":
                    output_data = self._process_nested(operation, input_data)
                else:  # "single"
                    output_data = operation(input_data)

            # Handle output storage
            output_config = step_config.get("output", {})
            if "storage" in output_config:
                self.data_store.store_step_output(
                    step_name=f"{step_name}_{step_context}",
                    data=output_data,
                    storage_config=output_config["storage"],
                    metadata={"step_context": step_context}
                )

            return output_data

        except Exception as e:
            self.logger.error(f"Error during execution of step '{step_name}': {e}")
            raise

    def run_pipeline(self, input_path, output_file):
        """Runs the pipeline as defined in the YAML configuration."""
        try:
            current_data = input_path

            for step_config in self.pipeline_config["steps"]:
                # Log in YAML format for readability
                self.logger.debug(f"Starting pipeline step:\n{yaml.dump(step_config, default_flow_style=False)}")
                current_data = self._process_nested(step_config, current_data)

            # Final output in JSON
            write_json(current_data, output_file)
            self.logger.info(f"Pipeline execution completed. Final output written to {output_file}.")

        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            raise

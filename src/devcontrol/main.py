import json
import os
from utils.io_utils import read_json, write_json, load_function
from utils.logger import setup_logger
from operations import process_list, process_dict, process_directory, process_nested

# Paths for working directories and logs
working_dir = '/home/don/Documents/Temp/WW990/structure/'
logs_dir = os.path.join(working_dir, 'logs/')
input_dir = os.path.join(working_dir, 'input/')
intermediates_dir = os.path.join(working_dir, 'intermediates/')


class PipelineManager:
    def __init__(self, config_file):
        self.pipeline_log = os.path.join(logs_dir, 'pipeline.log')
        self.logger = setup_logger("pipeline", self.pipeline_log)
        self.pipeline_config = self._load_config(config_file)
        self.intermediate_folder = intermediates_dir

    def _load_config(self, config_file):
        """Loads the pipeline configuration from a file."""
        try:
            with open(config_file, 'r') as file:
                config = json.load(file)
                self.logger.debug(f"Loaded pipeline configuration: {json.dumps(config, indent=2)}")
                return config["pipeline"]
        except Exception as e:
            self.logger.error(f"Failed to load configuration file {config_file}: {e}")
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

    def execute_operation(self, step_config, input_data, step_context):
        """Executes a single operation as defined in the pipeline configuration."""
        skip_step = step_config.get("skip_step", False)
        step_name = step_config["step_name"]
        module = step_config["module"]
        function_name = step_config["function"]

        self.logger.info(f"Executing step '{step_name}' using {module}.{function_name} in context '{step_context}'.")

        try:
            if skip_step:
                return input_data  # output data = input data with no intermediate store

            # Dynamically load the function
            operation = load_function(module, function_name)

            # Log structure of input data
            self.logger.debug(f"Input data for step '{step_name}': {type(input_data)}")

            output_data = operation(input_data)

            # Write intermediate data if enabled for this step
            if step_config.get("use_intermediate_file", False):
                output_file = os.path.join(self.intermediate_folder, f"{step_name}_{step_context}_output.json")
                tmp_file_path = os.path.join("/tmp", f"{step_name}_output.json")

                # Simulate writing to the /tmp directory
                with open(tmp_file_path, "w") as tmp_file:
                    json.dump(output_data, tmp_file)

                # Write metadata JSON pointing to the /tmp file
                metadata = {"filename": tmp_file_path, "indirect_reference": True}
                with open(output_file, "w") as file:
                    json.dump(metadata, file)

                self.logger.info(f"Intermediate data written to {output_file} referencing {tmp_file_path}.")

            return output_data
        except Exception as e:
            self.logger.error(f"Error during execution of step '{step_name}': {e}")
            raise

    def run_pipeline(self, input_path, output_file):
        """Runs the pipeline as defined in the configuration."""
        try:
            input_data = input_path  # Pass raw input path to the first step
            self.logger.debug(f"Pipeline starting with raw input: {input_data}")

            for step_config in self.pipeline_config:
                self.logger.debug(f"Starting pipeline step: {json.dumps(step_config, indent=2)}")
                input_data = self._process_nested(step_config, input_data)

            write_json(input_data, output_file)
            self.logger.info(f"Pipeline execution completed. Final output written to {output_file}.")
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            raise

if __name__ == "__main__":
    pipeline = PipelineManager(config_file=os.path.join('.', "config/pipeline_config.json"))
    infile = os.path.join(input_dir, "/tmp/tmp45cvctw7.json")
    outfile = os.path.join(intermediates_dir, "final_output.json")
    pipeline.run_pipeline(infile, outfile)

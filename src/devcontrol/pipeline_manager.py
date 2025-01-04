import os
import json
import yaml
import logging
from utils.logger import setup_logger
from utils.io_utils import read_json, write_json, load_function, read_yaml
from operations import process_list, process_dict, process_directory, process_nested
from user_level_operations import extract_urls
# from data_management.pipeline_data_store import PipelineDataStore
from data_management.dummy_data_store import DummyDataStore as PipelineDataStore
import traceback


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

            # Setup logging
            self.pipeline_log = os.path.join(".", self.config['paths']["logs_dir"], "pipeline_log.log")
            self.logger = setup_logger("pipeline", self.pipeline_log)

            # Add debug output
            self.logger.debug("PipelineManager initialized")
            self.logger.debug(f"Pipeline log location: {self.pipeline_log}")
            self.logger.debug(f"Config: {self.config}")
            self.logger.debug(f"Pipeline config: {self.pipeline_config}")
            self.logger.info("PipelineManager ready")

            # Initialize data store once with all configs
            base_path = os.path.join(".", self.config['paths'].get("data_dir", "pipeline_data"))
            db_connection_string = None
            if self.config.get('database', {}).get('enabled', False):
                db_connection_string = self.config['database']['connection_string']

            self.data_store = PipelineDataStore(
                self.logger,
                base_path=base_path,
                db_connection_string=db_connection_string
            )

            # Set execution parameters
            self.max_iterations = self.config.get('execution', {}).get('max_iterations', None)
            self.current_iteration = 0

        except Exception as e:
            print(f"Error initializing PipelineManager: {e}")
            raise

    def run_pipeline(self, input_path, output_file):
        """Runs the pipeline as defined in the YAML configuration."""
        try:
            current_data = input_path
            for step_config in self.pipeline_config["steps"]:
                self.logger.debug(f"Starting step: {step_config['step_name']}")

                # Handle skipped steps
                if step_config.get("process_mode") == "none":
                    self.logger.debug(f"Step {step_config['step_name']} marked for skip")
                    if "output" in step_config:
                        current_data = self._handle_output(step_config, current_data)
                    continue

                # Process the step
                current_data = self._process_step(step_config, current_data)

                # Handle generator if configured
                if step_config.get("generator", {}).get("enabled", False):
                    current_data = self._wrap_in_generator(current_data, step_config)

            # Handle final output based on pipeline config
            output_config = self.pipeline_config.get("output", {})
            self._write_output(current_data, output_file, output_config)

            self.logger.info(f"Pipeline execution completed. Output written to {output_file}")

        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            self.logger.error(f"Run Pipeline: {traceback.format_exc()}")
            raise

    def _process_step(self, step_config, input_data):
        """Handles single step processing"""
        step_name = step_config["step_name"]

        # Handle input processing
        processed_input = self._handle_input(step_config, input_data)

        # Process data based on mode
        if step_config.get("process_mode") == "nested":
            output_data = self._process_nested(step_config, processed_input)
        else:
            output_data = self.execute_operation(step_config, processed_input, step_name)

        return output_data

    def _process_nested(self, step_config, input_data):
        """Handles processing of nested data structures."""
        step_name = step_config["step_name"]
        explicit_input = step_config.get("explicit_input", None)

        try:
            # Handle explicit input with type checking
            if explicit_input:
                input_data = self._handle_explicit_input(explicit_input, step_config)

            # Process based on input type
            if isinstance(input_data, dict):
                self.logger.info(f"{step_name}: Processing dictionary structure.")
                output_data = {
                    key: self._process_nested(step_config, value)
                    for key, value in input_data.items()
                }
            elif isinstance(input_data, list):
                self.logger.info(f"{step_name}: Processing list of items.")
                operation = load_function(step_config["module"], step_config["function"])
                output_data = [operation(item) for item in input_data]
            elif isinstance(input_data, str) and os.path.isdir(input_data):
                output_data = self._process_directory(step_config, input_data)
            else:
                # For terminal values, directly call operation
                operation = load_function(step_config["module"], step_config["function"])
                output_data = operation(input_data)

            return output_data

        except Exception as e:
            self.logger.error(f"Error in step '{step_name}': {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def execute_operation(self, step_config, input_data, step_context):
        """Execute operation with input/output handling"""
        step_name = step_config["step_name"]
        self.logger.info(f"Begin Processing Step: {step_name}")

        try:
            # Load operation function if needed
            operation = None
            if step_config.get("process_mode") != "none":
                operation = load_function(step_config["module"], step_config["function"])

            # Process the data
            if step_config.get("process_mode") == "none":
                output_data = input_data
            else:
                output_data = operation(input_data)

            # Handle output storage
            output_data = self._handle_output(step_config, output_data, step_context)

            return output_data

        except Exception as e:
            self.logger.error(f"Error during execution of step '{step_name}': {e}")
            self.logger.error(f"Execute operation: {traceback.format_exc()}")
            raise

    def _handle_input(self, step_config, input_data):
        """Processes input based on configuration"""
        input_config = step_config.get("input", {"mode": "previous"})
        input_mode = input_config["mode"]

        if input_mode == "storage":
            return self.data_store.get_step_input(
                step_name=step_config["step_name"],
                storage_config=input_config["storage"]
            )
        elif input_mode == "explicit_input":
            return self._handle_explicit_input(input_config.get("storage", {}).get("value"), step_config)
        elif input_mode in ["previous", "passthrough"]:
            return input_data
        else:
            raise ValueError(f"Unknown input mode: {input_mode}")

    def _handle_explicit_input(self, input_value, step_config):
        """Handles explicit input with type checking"""
        if isinstance(input_value, str):
            if input_value.startswith(('http://', 'https://')):
                return input_value
            elif os.path.exists(input_value):
                ext = os.path.splitext(input_value)[1].lower()
                if ext == ".json":
                    return json.load(open(input_value))
                return input_value
        return input_value

    def _handle_output(self, step_config, data, step_context=None):
        """Handles output storage and returns processed data"""
        output_config = step_config.get("output", {})
        # if "storage" in output_config:
        #     self.data_store.store_step_output(
        #         step_name=f"{step_config['step_name']}_{step_context}" if step_context else step_config['step_name'],
        #         data=data,
        #         storage_config=output_config["storage"],
        #         metadata={"step_context": step_context} if step_context else None
        #     )
        return data

    def _wrap_in_generator(self, data, step_config):
        """Creates generator wrapper for step output"""
        filter_expr = step_config.get("generator", {}).get("filter")
        generator = self.create_generator(data, filter_expr)

        # Debug generator flow if debug logging enabled
        if self.logger.isEnabledFor(logging.DEBUG):
            self.debug_generator_flow(step_config, generator)

        return generator

    def create_generator(self, data, filter_expr=None):
        """Creates a generator from data with optional filtering."""
        count = 0
        if isinstance(data, (list, tuple)):
            for item in data:
                if self.max_iterations and count >= self.max_iterations:
                    break

                if filter_expr:
                    try:
                        filter_func = eval(f"lambda x: {filter_expr}")
                        if filter_func(item):
                            yield item
                            count += 1
                    except Exception as e:
                        self.logger.error(f"Error in filter expression: {e}")
                        raise
                else:
                    yield item
                    count += 1
        else:
            yield data

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
                    processed = self.execute_operation(
                        step_config,
                        file_input,
                        os.path.relpath(file_path, input_path)
                    )
                    output_data[os.path.relpath(file_path, input_path)] = processed

        return output_data

    def debug_generator_flow(self, step_config, generator):
        """Preview generator output without processing."""
        step_name = step_config["step_name"]
        preview = []
        for i, item in enumerate(generator):
            if i >= 5:  # Preview first 5 items
                break
            preview.append(item)

        self.logger.debug(f"{step_name}: Generator preview: {preview}")
        return preview

    # In pipeline_manager.py

    def _write_output(self, data, output_file, output_config):
        """Writes output in specified format"""
        output_format = output_config.get("format", "json")

        # Convert generator to list if needed
        if hasattr(data, '__iter__') and hasattr(data, '__next__'):
            data = list(data)

        if output_format == "json":
            write_json(data, output_file)
        else:
            self.logger.warning(f"Unsupported output format {output_format}, defaulting to JSON")
            write_json(data, output_file)

    def _debug_step(self, step_name, input_data, output_data):
        """Logs debug information for step processing"""
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"""
Step: {step_name}
Input Type: {type(input_data)}
Input Preview: {str(input_data)[:100]}
Output Type: {type(output_data)}
Output Preview: {str(output_data)[:100]}
""")

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
#                 str, bytes, os.PathLike)) else pipeline_config
#
#             self.pipeline_log = os.path.join(".", self.config['paths']["logs_dir"], "pipeline_log.log")
#             self.logger = setup_logger("pipeline", self.pipeline_log)
#             self.data_store = PipelineDataStore(self.logger,
#                 base_path=os.path.join(".", self.config['paths'].get("data_dir", "pipeline_data")))
#             # Initialize data store with database support if enabled
#             db_connection_string = None
#             if self.config.get('database', {}).get('enabled', False):
#                 db_connection_string = self.config['database']['connection_string']
#
#             self.data_store = PipelineDataStore(self.logger,
#                 base_path=os.path.join(".", self.config['paths'].get("data_dir", "pipeline_data")),
#                 db_connection_string=db_connection_string
#             )
#             self.max_iterations = self.config.get('execution', {}).get('max_iterations', None)
#             self.current_iteration = 0
#         except Exception as e:
#             print(f"Error initializing PipelineManager: {e}")
#             raise
#
#     def _process_directory(self, step_config, input_path):
#         """Processes a directory, including nested subdirectories."""
#         step_name = step_config["step_name"]
#         output_data = {}
#
#         self.logger.info(f"{step_name}: Processing directory {input_path}.")
#         for root, _, files in os.walk(input_path):
#             for filename in files:
#                 file_path = os.path.join(root, filename)
#                 if os.path.isfile(file_path):
#                     self.logger.debug(f"Processing file {file_path}.")
#                     file_input = read_json(file_path)
#                     processed = self.execute_operation(step_config, file_input, os.path.relpath(file_path, input_path))
#                     output_data[os.path.relpath(file_path, input_path)] = processed
#
#         return output_data
#
#     def _process_nested(self, step_config, input_data):
#         """Handles processing dynamically based on the actual input data type."""
#         step_name = step_config["step_name"]
#         process_mode = step_config.get("process_mode", None)
#         skip = False
#         if process_mode == 'none':
#             skip = True
#         # explicit_input means that the input to the function is given explicitly in the YAML
#         # if it is a path to a json file, that file is loaded
#         explicit_input = step_config.get("explicit_input", None)
#
#         try:
#             # Handle explicit input first
#             if explicit_input:
#                 if isinstance(explicit_input, str):
#                     if explicit_input.startswith(('http://', 'https://')):
#                         input_data = explicit_input
#                     elif os.path.exists(explicit_input):
#                         input_extension = os.path.splitext(explicit_input)[1].lower()
#                         input_data = json.load(open(explicit_input)) if input_extension == ".json" else explicit_input
#                     else:
#                         raise FileNotFoundError(f"Explicit input {explicit_input} not found.")
#
#             # Process the input based on its type
#             if not skip:
#                 if isinstance(input_data, dict):
#                     self.logger.info(f"{step_name}: Processing dictionary structure.")
#                     output_data = {
#                         key: self._process_nested(step_config, value)
#                         for key, value in input_data.items()
#                     }
#                 elif isinstance(input_data, list):
#                     self.logger.info(f"{step_name}: Processing list of items.")
#                     # For lists, directly call operation on each item
#                     operation = load_function(step_config["module"], step_config["function"])
#                     output_data = [operation(item) for i, item in enumerate(input_data)]
#                 else:
#                     # For terminal values, directly call operation
#                     operation = load_function(step_config["module"], step_config["function"])
#                     output_data = operation(input_data)
#             else:
#                 output_data = input_data
#
#             # Apply generator processing to output if enabled
#             generator_config = step_config.get("generator", {})
#             if generator_config.get("enabled", False):
#                 if isinstance(output_data, (list, tuple)):
#                     filter_expr = generator_config.get("filter")
#                     return self._process_generator(step_config, self.create_generator(output_data, filter_expr))
#                 else:
#                     self.logger.warning(f"{step_name}: Generator enabled but output is not a sequence")
#                     return output_data
#
#             return output_data
#
#         except Exception as e:
#             self.logger.error(f"Error in step '{step_name}': {e}")
#             self.logger.error(f"Traceback: {traceback.format_exc()}")
#             raise
#     def create_generator(self, data, filter_expr=None):
#         """Creates a generator from data with optional filtering."""
#         count = 0
#         if isinstance(data, (list, tuple)):
#             for item in data:
#                 if self.max_iterations and count >= self.max_iterations:
#                     break
#
#                 if filter_expr:
#                     try:
#                         filter_func = eval(f"lambda x: {filter_expr}")
#                         if filter_func(item):
#                             yield item
#                             count += 1
#                     except Exception as e:
#                         self.logger.error(f"Error in filter expression: {e}")
#                         raise
#                 else:
#                     yield item
#                     count += 1
#         else:
#             yield data
#
#     def _process_generator(self, step_config, generator):
#         """Process items from a generator one at a time."""
#         step_name = step_config["step_name"]
#         results = []
#
#         try:
#             for item in generator:
#                 if self.max_iterations and self.current_iteration >= self.max_iterations:
#                     self.logger.info(f"Reached maximum iterations ({self.max_iterations})")
#                     break
#
#                 self.logger.info(f"{step_name}: Processing item {self.current_iteration + 1}")
#                 processed = self.execute_operation(step_config, item, f"{step_name}_{self.current_iteration}")
#                 results.append(processed)
#                 self.current_iteration += 1
#
#             return results
#         except Exception as e:
#             self.logger.error(f"Error in generator processing: {e}")
#             raise
#
#     def execute_operation(self, step_config, previous_step_data, step_context):
#         """Execute operation with simplified processing modes"""
#         step_name = step_config["step_name"]
#         self.logger.info(f"Begin Processing Step: {step_name}")
#         input_config = step_config.get("input", {"mode": "previous"})
#         generator_enabled = step_config.get("generator", {}).get("enabled", False)
#
#         try:
#             # Handle input mode
#             if input_config["mode"] == "storage":
#                 input_data = self.data_store.get_step_input(
#                     step_name=step_name,
#                     storage_config=input_config["storage"]
#                 )
#             elif input_config["mode"] == "explicit_input":
#                 # Handle explicit input, including URLs
#                 if "storage" in input_config and "value" in input_config["storage"]:
#                     input_data = input_config["storage"]["value"]
#                 else:
#                     input_data = previous_step_data
#             elif input_config["mode"] == "passthrough":
#                 input_data = previous_step_data
#             else:  # "previous"
#                 input_data = previous_step_data
#
#             # Load operation function
#             if step_config.get("process_mode") != "none":
#                 operation = load_function(step_config["module"], step_config["function"])
#                 if generator_enabled and not callable(operation):
#                     raise ValueError(f"Operation {step_config['function']} must be callable for generator processing")
#
#             # Handle processing mode
#             process_mode = step_config.get("process_mode", "nested")
#             if process_mode == "none":
#                 output_data = input_data
#             elif process_mode == "nested":
#                 output_data = self._process_nested(step_config, input_data)
#             else:  # "single"
#                 output_data = operation(input_data)
#
#             # Handle output storage
#             output_config = step_config.get("output", {})
#             if "storage" in output_config:
#                 self.data_store.store_step_output(
#                     step_name=f"{step_name}_{step_context}",
#                     data=output_data,
#                     storage_config=output_config["storage"],
#                     metadata={"step_context": step_context}
#                 )
#
#             return output_data
#
#         except Exception as e:
#             self.logger.error(f"Error during execution of step '{step_name}': {e}")
#             self.logger.error(f"Execute operation: {traceback.format_exc()}")
#             if generator_enabled:
#                 self.logger.error("Generator processing failed - stopping iteration")
#             raise
#
#     def run_pipeline(self, input_path, output_file):
#         """Runs the pipeline as defined in the YAML configuration."""
#         try:
#             current_data = input_path
#             for step_config in self.pipeline_config["steps"]:
#                 # Log in YAML format for readability
#                 self.logger.debug(f"Starting pipeline step:\n{yaml.dump(step_config, default_flow_style=False)}")
#                 current_data = self._process_nested(step_config, current_data)
#
#             # Final output in JSON
#             write_json(current_data, output_file)
#             self.logger.info(f"Pipeline execution completed. Final output written to {output_file}.")
#
#         except Exception as e:
#             self.logger.error(f"Pipeline execution failed: {e}")
#             self.logger.error(f"Run Pipeliner: {traceback.format_exc()}")
#             raise
#
#     def debug_generator_flow(self, step_config, generator):
#         """Preview generator output without processing."""
#         step_name = step_config["step_name"]
#         preview = []
#         for i, item in enumerate(generator):
#             if i >= 5:  # Preview first 5 items
#                 break
#             preview.append(item)
#
#         self.logger.debug(f"{step_name}: Generator preview: {preview}")
#         return preview

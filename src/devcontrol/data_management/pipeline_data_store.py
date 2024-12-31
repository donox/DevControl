from typing import Any, Dict, Optional, List
import pandas as pd
import numpy as np
import json
import os
from PIL import Image
from datetime import datetime
import io
from devcontrol.utils.io_utils import read_yaml, read_json, write_json, load_function


class PipelineDataStore:
    def __init__(self, base_path: str = "./pipeline_data"):
        self.memory_store = {}
        self.base_path = base_path
        self.type_converters = {
            "dataframe": {
                "to_file": self._df_to_file,
                "from_file": self._file_to_df,
                "to_memory": self._df_to_memory,
                "from_memory": self._memory_to_df
            },
            "image_list": {
                "to_file": self._images_to_file,
                "from_file": self._file_to_images,
                "to_memory": self._images_to_memory,
                "from_memory": self._memory_to_images
            },
            # Add more type converters as needed
        }

    def store_step_output(self, step_name: str, data: Any,
                          storage_config: Dict, metadata: Optional[Dict] = None) -> None:
        """Store step output according to storage configuration"""
        storage_type = storage_config.get("type", "memory")
        data_format = storage_config.get("format", "raw")

        if storage_type == "memory":
            self._store_in_memory(step_name, data, data_format, metadata)
        elif storage_type == "file":
            self._store_in_file(step_name, data, storage_config, metadata)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")

    def get_step_input(self, step_name: str,
                       storage_config: Dict) -> Any:
        """Retrieve step input according to storage configuration"""
        storage_type = storage_config.get("type", "memory")
        data_format = storage_config.get("format", "raw")

        if storage_type == "memory":
            return self._get_from_memory(step_name, data_format)
        elif storage_type == "file":
            return self._get_from_file(step_name, storage_config)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")

    def _store_in_memory(self, step_name: str, data: Any,
                         data_format: str, metadata: Optional[Dict]) -> None:
        """Store data in memory with format conversion"""
        if data_format in self.type_converters:
            data = self.type_converters[data_format]["to_memory"](data)
        self.memory_store[step_name] = {
            "data": data,
            "format": data_format,
            "metadata": metadata
        }

    def _store_in_file(self, step_name: str, data: Any,
                       storage_config: Dict, metadata: Optional[Dict]) -> None:
        """Store data in file with format conversion - using JSON for data storage"""
        data_format = storage_config.get("format", "raw")
        location = storage_config.get("location", os.path.join(self.base_path, step_name))
        os.makedirs(os.path.dirname(location), exist_ok=True)

        if data_format in self.type_converters:
            self.type_converters[data_format]["to_file"](data, location)
        else:
            # Default JSON serialization for raw data
            write_json({"data": data, "metadata": metadata}, location)

    def _df_to_file(self, df: pd.DataFrame, location: str) -> None:
        df.to_parquet(location + ".parquet")

    def _file_to_df(self, location: str) -> pd.DataFrame:
        return pd.read_parquet(location + ".parquet")

    def _df_to_memory(self, df: pd.DataFrame) -> Dict:
        return df.to_dict()

    def _memory_to_df(self, data: Dict) -> pd.DataFrame:
        return pd.DataFrame.from_dict(data)

    def _images_to_file(self, images: List, location: str) -> None:
        """Store list of images to files in a directory

        Args:
            images: List of PIL.Image objects or numpy arrays
            location: Base directory path for storage
        """
        os.makedirs(location, exist_ok=True)

        # Store metadata about the image collection
        metadata = {
            "count": len(images),
            "format": "png",
            "timestamp": datetime.now().isoformat()
        }

        with open(os.path.join(location, "metadata.json"), "w") as f:
            json.dump(metadata, f)

        # Save each image
        for idx, img in enumerate(images):
            if isinstance(img, np.ndarray):
                img = Image.fromarray(img)
            if not isinstance(img, Image.Image):
                raise ValueError(f"Image {idx} is not a PIL Image or numpy array")

            filename = os.path.join(location, f"image_{idx:04d}.png")
            img.save(filename, format="PNG")

    def _file_to_images(self, location: str) -> List[Image.Image]:
        """Load images from a directory into a list

        Args:
            location: Directory path containing stored images

        Returns:
            List of PIL.Image objects
        """
        # Check metadata
        metadata_path = os.path.join(location, "metadata.json")
        if not os.path.exists(metadata_path):
            raise FileNotFoundError(f"No metadata file found in {location}")

        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        images = []
        for idx in range(metadata["count"]):
            filename = os.path.join(location, f"image_{idx:04d}.png")
            if not os.path.exists(filename):
                raise FileNotFoundError(f"Missing image file {filename}")

            img = Image.open(filename)
            images.append(img)

        return images

    def _images_to_memory(self, images: List[Image.Image]) -> List[bytes]:
        """Convert list of images to bytes for memory storage

        Args:
            images: List of PIL.Image objects

        Returns:
            List of bytes objects, each containing a PNG-encoded image
        """
        image_bytes = []
        for img in images:
            buffer = io.BytesIO()
            img.save(buffer, format="PNG")
            image_bytes.append(buffer.getvalue())
        return image_bytes

    def _memory_to_images(self, data: List[bytes]) -> List[Image.Image]:
        """Convert list of bytes back to images

        Args:
            data: List of bytes objects containing PNG-encoded images

        Returns:
            List of PIL.Image objects
        """
        images = []
        for img_bytes in data:
            buffer = io.BytesIO(img_bytes)
            img = Image.open(buffer)
            images.append(img)
        return images

    def _get_from_file(self, step_name: str, storage_config: Dict) -> Any:
        """Retrieve data from file storage using appropriate type converter"""
        data_format = storage_config.get("format", "raw")
        location = storage_config.get("location", os.path.join(self.base_path, step_name))

        if not os.path.exists(location):
            raise FileNotFoundError(f"Storage location not found: {location}")

        if data_format in self.type_converters:
            return self.type_converters[data_format]["from_file"](location)

        # Default JSON deserialization for raw data
        stored_data = read_json(location)
        return stored_data.get("data")

    # Add more conversion methods for images, etc.

class DummyDataStore:
    def __init__(self, logger, base_path, db_connection_string=None):
        self.logger = logger
        self.base_path = base_path
        self.db_enabled = db_connection_string is not None
        self.memory_store = {}  # Simple dict to simulate storage

    def store_step_output(self, step_name, data, storage_config, metadata=None):
        """Mock storage based on storage_config type"""
        storage_type = storage_config.get("type", "memory")
        if storage_type == "database":
            key = f"db_{step_name}"
            self.memory_store[key] = {
                'data': data,
                'metadata': metadata
            }
            self.logger.debug(f"Stored in dummy DB: {key}")
        else:
            # Handle file/memory storage as before
            pass

    def get_step_input(self, step_name, storage_config):
        """Mock retrieval based on storage_config type"""
        storage_type = storage_config.get("type", "memory")
        if storage_type == "database":
            key = f"db_{step_name}"
            stored = self.memory_store.get(key, {})
            self.logger.debug(f"Retrieved from dummy DB: {key}")
            return stored.get('data')
        else:
            # Handle file/memory retrieval as before
            pass

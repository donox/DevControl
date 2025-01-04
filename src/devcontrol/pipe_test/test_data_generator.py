# pipe_test/test_data_generator.py
import random
import string
import json
import os
from datetime import datetime, timedelta


class TestDataGenerator:
    @staticmethod
    def generate_urls(count=5):
        """Generate list of mock URLs"""
        base_urls = [
            "https://example.com/data/",
            "https://test.org/files/",
            "https://sample.net/docs/"
        ]
        extensions = ['.pdf', '.zip', '.json', '.csv']

        urls = []
        for i in range(count):
            base = random.choice(base_urls)
            ext = random.choice(extensions)
            filename = f"file_{i}{ext}"
            urls.append(f"{base}{filename}")

        return urls

    @staticmethod
    def generate_file_paths(directory, count=5):
        """Generate list of mock file paths"""
        if not os.path.exists(directory):
            os.makedirs(directory)

        paths = []
        for i in range(count):
            filename = f"test_file_{i}.txt"
            path = os.path.join(directory, filename)
            # Create empty file
            with open(path, 'w') as f:
                f.write(f"Test content {i}")
            paths.append(path)

        return paths

    @staticmethod
    def generate_nested_dict(depth=3, width=2):
        """Generate nested dictionary structure"""
        if depth == 0:
            return f"value_{random.randint(1, 100)}"

        result = {}
        for i in range(width):
            key = f"key_{depth}_{i}"
            result[key] = TestDataGenerator.generate_nested_dict(depth - 1, width)
        return result

    @staticmethod
    def generate_nested_list(depth=3, width=2):
        """Generate nested list structure"""
        if depth == 0:
            return f"item_{random.randint(1, 100)}"

        return [TestDataGenerator.generate_nested_list(depth - 1, width)
                for _ in range(width)]

    @staticmethod
    def generate_records(count=5):
        """Generate list of record dictionaries"""
        records = []
        for i in range(count):
            record = {
                'id': i,
                'name': f"Item_{i}",
                'value': random.randint(1, 1000),
                'timestamp': (datetime.now() + timedelta(days=i)).isoformat()
            }
            records.append(record)
        return records

    @staticmethod
    def generate_mixed_data():
        """Generate mixed data types structure"""
        return {
            'string': ''.join(random.choices(string.ascii_letters, k=10)),
            'integer': random.randint(1, 100),
            'float': random.uniform(1.0, 100.0),
            'list': [random.randint(1, 10) for _ in range(3)],
            'nested': {
                'a': [1, 2, 3],
                'b': {'x': 1, 'y': 2}
            }
        }

    @staticmethod
    def save_test_data(data, filename):
        """Save generated data to file"""
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def generate_chunked_data(total_size=100, chunk_size=10):
        """Generate data in chunks"""
        data = []
        for i in range(0, total_size, chunk_size):
            chunk = []
            for j in range(chunk_size):
                if i + j < total_size:
                    item = {
                        'index': i + j,
                        'value': random.randint(1, 1000),
                        'text': f"Item_{i + j}"
                    }
                    chunk.append(item)
            data.append(chunk)
        return data


# Example usage and test function
def create_test_dataset():
    """Create a complete test dataset"""
    generator = TestDataGenerator()
    test_data = {
        'urls': generator.generate_urls(),
        'nested_dict': generator.generate_nested_dict(),
        'nested_list': generator.generate_nested_list(),
        'records': generator.generate_records(),
        'mixed_data': generator.generate_mixed_data(),
        'chunked_data': generator.generate_chunked_data()
    }

    # Save to test data directory
    os.makedirs('test_data', exist_ok=True)
    generator.save_test_data(test_data, 'test_data/test_dataset.json')

    # Generate file paths
    file_paths = generator.generate_file_paths('test_data/test_files')

    return test_data, file_paths


if __name__ == "__main__":
    # Create test dataset and print summary
    data, paths = create_test_dataset()
    print("Test data generated:")
    print(f"Number of URLs: {len(data['urls'])}")
    print(f"Nested dict depth: {len(str(data['nested_dict']))}")
    print(f"Number of records: {len(data['records'])}")
    print(f"Number of file paths: {len(paths)}")
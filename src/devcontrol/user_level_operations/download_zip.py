import requests
from pathlib import Path
from contextlib import closing


def download_next_url(url_generator, **kwargs):
    try:
        for url in url_generator:
            next_url = next(url_generator)
            result = download_zip(next_url, **kwargs)
            yield result
    except StopIteration:
        print("No more URLs available in the generator")
        return None


def download_zip(url, **kwargs):
    """
    Download a zip file from a URL and save it to the specified directory if not already downloaded.

    Args:
        url (str): URL of the zip file to download.
        kwargs (dictionary): Dictionary containing directory (key: directory)

    Returns:
        str: Path of the downloaded or existing file, or None if an error occurred.
    """
    try:
        # Set default directory
        if 'directory' not in kwargs or kwargs['directory'] is None:
            directory = "./output/zip_files"
        else:
            directory = kwargs["directory"]
        dir_path = Path(directory)
        dir_path.mkdir(parents=True, exist_ok=True)

        # Extract the base file name from the URL, removing the `.zip` extension
        filename = Path(url).stem + ".zip" # Gets "2024_TEOS_XML_01A" from the example URL
        filepath = dir_path / filename

        # Check if the file already exists
        if filepath.exists():
            print(f"File already exists, skipping download: {filepath}")
            return str(filepath)

        # Use context managers for session and response
        with closing(requests.Session()) as session:
            with session.get(url, stream=True, timeout=(3.05, 27)) as response:
                response.raise_for_status()

                # Write file with progress
                total_size = int(response.headers.get('content-length', 0))
                with filepath.open('wb') as f:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:  # Filter out keep-alive chunks
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                print(f"Downloading {filename}: {downloaded / total_size:.2%} complete", end="\r")

        print(f"\nDownload completed: {filepath}")
        return str(filepath)

    except Exception as e:
        print(f"Error downloading {url}: {str(e)}")
        return None

        return None
# import requests
# import os
# from urllib.parse import urlparse
#
#
# def download_zip(url, directory):
#     """
#     Download a zip file from a URL and save it to specified directory
#
#     Args:
#         url: URL of the zip file to download
#         directory: Directory to save the file in
#
#     Returns:
#         str: Name of downloaded file, or None if download failed
#     """
#     try:
#         # Create directory if it doesn't exist
#         os.makedirs(directory, exist_ok=True)
#
#         # Get filename from URL
#         filename = 'zipped_file.zip'
#         filepath = os.path.join(directory, filename)
#
#         # Download the file
#         response = requests.get(url, stream=True)
#         response.raise_for_status()  # Raise exception for bad status codes
#
#         # Save the file
#         with open(filepath, 'wb') as f:
#             for chunk in response.iter_content(chunk_size=8192):
#                 f.write(chunk)
#             f.close()
#
#         foo=3
#         return filename
#
#     except Exception as e:
#         print(f"Error downloading {url}: {str(e)}")
#         return None
#
# # Example usage:
# # filename = download_zip('https://example.com/file.zip', '/path/to/directory')
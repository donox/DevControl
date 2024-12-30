from setuptools import setup, find_packages

setup(
    name="devcontrol",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        'process_990s'
        # copy dependencies from your requirements.txt
    ]
)
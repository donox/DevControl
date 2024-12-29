#!/usr/bin/env python3
import shlex
import logging
from logging.handlers import RotatingFileHandler
import subprocess

def setup_logger(name, log_file, level=logging.INFO):
    """Sets up a logger with a specified name and log file."""
    handler = logging.handlers.RotatingFileHandler(log_file, mode='w',  maxBytes=1024*1024,    # 1MB per file
            backupCount=3)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

# Example usage in other modules:
# logger = setup_logger("application", "logs/application.log")






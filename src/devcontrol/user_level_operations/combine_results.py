import os
import shutil

def create_directory(path):
    """
    Creates a directory if it doesn't already exist.
    """
    if not os.path.exists(path):
        os.makedirs(path)

def move_foreign_files(base_dir, summary_dir):
    """
    Moves files directly in any 'result' folder (but not in subdirectories) to 'summary/foreign'.

    Args:
        base_dir (str): The base directory containing the result folders.
        summary_dir (str): The summary directory where 'foreign' will be created.
    """
    foreign_dir = os.path.join(summary_dir, 'foreign')
    create_directory(foreign_dir)

    for year in ['2023', '2024']:
        year_path = os.path.join(base_dir, year, 'expanded_zip_files')
        if not os.path.exists(year_path):
            continue

        for folder in os.listdir(year_path):
            result_path = os.path.join(year_path, folder, 'result')
            if not os.path.exists(result_path):
                continue

            for item in os.listdir(result_path):
                item_path = os.path.join(result_path, item)
                if os.path.isfile(item_path):  # Move files directly in the result folder
                    shutil.move(item_path, foreign_dir)
                    # print(f"Moved file to foreign: {item_path}")


def combine_numeric_folders(base_dir, summary_dir):
    """
    Combines numeric folders from all 'result' directories into the 'summary' directory.

    Args:
        base_dir (str): The base directory containing the result folders.
        summary_dir (str): The summary directory to consolidate folders into.
    """
    for year in ['2023', '2024']:
        year_path = os.path.join(base_dir, year, 'expanded_zip_files')
        if not os.path.exists(year_path):
            continue

        for folder in os.listdir(year_path):
            result_path = os.path.join(year_path, folder, 'result')
            if not os.path.exists(result_path):
                continue

            for subfolder in os.listdir(result_path):
                subfolder_path = os.path.join(result_path, subfolder)
                if os.path.isdir(subfolder_path) and subfolder.isdigit():  # Only numeric subfolders
                    target_folder = os.path.join(summary_dir, subfolder)
                    create_directory(target_folder)

                    # Move all contents of the numeric folder to the target folder
                    for item in os.listdir(subfolder_path):
                        item_path = os.path.join(subfolder_path, item)
                        shutil.move(item_path, target_folder)
                        # print(f"Moved {item_path} to {target_folder}")
                else:
                    foo = 3
                    print(f"Non-numeric subfolder: path- {subfolder_path}", folder- {subfolder})

def process_summary(base_dir):
    """
    Processes the directories to create a summary directory structure.

    Args:
        base_dir (str): The base directory containing '2023' and '2024' directories.
    """
    # Create the summary directory
    summary_dir = os.path.join(base_dir, 'summary')
    create_directory(summary_dir)

    # Step 1: Move foreign files
    move_foreign_files(base_dir, summary_dir)

    # Step 2: Combine numeric subfolders
    combine_numeric_folders(base_dir, summary_dir)

    print(f"Summary processing completed. Check the 'summary' directory at {summary_dir}")

if __name__ == "__main__":
    # Replace with your actual base directory
    base_directory = "/home/don/Documents/Temp/dev990/"
    process_summary(base_directory)

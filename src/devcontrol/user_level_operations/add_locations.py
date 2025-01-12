import os
import xml.etree.ElementTree as ET


def add_location(file_path, year, result_folder_name):
    """
    Modifies the XML file by adding 'year' and 'location' attributes to the root <Results> element.

    Args:
        file_path (str): Path to the XML file to be modified.
        year (str): The year to add as an attribute.
        result_folder_name (str): The location (folder name) to add as an attribute.
    """
    try:
        # Parse the XML file
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Check if the root element is 'Results'
        if root.tag != 'Results':
            print(f"Skipping file: {file_path} (Root element is not 'Results')")
            return

        # Add attributes
        root.set('year', year)
        root.set('location', result_folder_name)

        # Write the modified XML back to the file
        tree.write(file_path, encoding='utf-8', xml_declaration=True)
        # print(f"Updated file: {file_path}")

    except ET.ParseError as e:
        print(f"Error parsing XML file: {file_path} | Error: {e}")
    except Exception as e:
        print(f"Error processing file: {file_path} | Error: {e}")

def process_directory(base_dir):
    """
    Iterates through the directory structure, processes files in 'result' folders
    and their subdirectories, and prints the total number of files processed.

    Args:
        base_dir (str): The base directory to start processing from.
    """
    # Directories of interest
    years_of_interest = ['2023', '2024']
    total_files_processed = 0

    for year in years_of_interest:
        year_path = os.path.join(base_dir, year, 'expanded_zip_files')
        if not os.path.exists(year_path):
            print(f"Year directory not found: {year_path}")
            continue

        for folder in os.listdir(year_path):
            folder_path = os.path.join(year_path, folder, 'result')
            if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
                print(f"Result folder not found: {folder_path}")
                continue
            print(f"Process year: {year} folder: {folder}")

            # Process all files recursively in the 'result' folder and its subdirectories
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    add_location(file_path, year, folder)
                    total_files_processed += 1

    # Print total number of files processed
    print(f"Total files processed: {total_files_processed}")

if __name__ == "__main__":
    # Replace with your actual base directory
    base_directory = "/home/don/Documents/Temp/dev990/"
    process_directory(base_directory)

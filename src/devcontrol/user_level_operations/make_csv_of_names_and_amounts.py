import os
import csv
import xml.etree.ElementTree as ET

def extract_xml_data(file_path):
    """
    Extracts required data from the XML file.

    Args:
        file_path (str): Path to the XML file.

    Returns:
        dict: Extracted data with keys 'BusinessName', 'FMVAssetsEOYAmt', and 'QualifyingDistributionsAmt'.
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        # XPath-like extraction
        business_name = root.find('BusinessNameLine1Txt').text
        fmv_assets = root.find('FMVAssetsEOYAmt').text
        if fmv_assets.isdigit():
            fmv_assets = int(fmv_assets)
        else:
            fmv_assets = -999
        qualifying_distributions = root.find('PFQualifyingDistributionsGrp')
        if qualifying_distributions:
            qualifying_distributions = qualifying_distributions.findtext('QualifyingDistributionsAmt')
        else:
            qualifying_distributions = -999
        if not qualifying_distributions:
            qualifying_distributions = -999

        # Convert amounts to integers
        try:
            fmv_assets = int(float(fmv_assets))
        except ValueError:
            fmv_assets = -999

        try:
            qualifying_distributions = int(float(qualifying_distributions))
        except ValueError:
            qualifying_distributions = -999

        return {
            "BusinessName": business_name,
            "FMVAssetsEOYAmt": fmv_assets,
            "QualifyingDistributionsAmt": qualifying_distributions
        }
    except ET.ParseError as e:
        print(f"Error parsing XML file {file_path}: {e}")
        return {"BusinessName": "Error", "FMVAssetsEOYAmt": 0, "QualifyingDistributionsAmt": 0}

def sanitize_city_name(city):
    """
    Converts multi-word city names to be joined by underscores.

    Args:
        city (str): The city name to sanitize.

    Returns:
        str: Sanitized city name.
    """
    return city.replace(" ", "_")

def process_files_by_msa(base_dir, output_csv):
    """
    Walks through the files in the files_by_msa directory, extracts data from XML files,
    and writes to a CSV file.

    Args:
        base_dir (str): The base directory of files_by_msa.
        output_csv (str): Path to the output CSV file.
    """
    rows = []

    # Walk through the files_by_msa directory
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.xml'):  # Process only XML files
                file_path = os.path.join(root, file)

                # Extract city, zipcode, and filename stem from the file path
                relative_path = os.path.relpath(file_path, base_dir)
                parts = relative_path.split(os.sep)
                if len(parts) < 3:
                    print(f"Skipping unexpected file path format: {file_path}")
                    continue

                city = sanitize_city_name(parts[-3])
                zipcode = parts[-2]
                filename_stem = os.path.splitext(file)[0]

                # Extract data from the XML file
                data = extract_xml_data(file_path)

                # Append the row
                rows.append({
                    "City": city,
                    "Zipcode": zipcode,
                    "Filename": filename_stem,
                    "BusinessName": data["BusinessName"],
                    "FMVAssetsEOYAmt": data["FMVAssetsEOYAmt"],
                    "QualifyingDistributionsAmt": data["QualifyingDistributionsAmt"]
                })

    # Write the data to a CSV file
    with open(output_csv, mode='w', newline='', encoding='utf-8') as csv_file:
        fieldnames = ["City", "Zipcode", "Filename", "BusinessName", "FMVAssetsEOYAmt", "QualifyingDistributionsAmt"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV file has been created: {output_csv}")

if __name__ == "__main__":
    # Define the base directory and output CSV file
    base_directory = "/home/don/Documents/Temp/dev990/summary/files_by_msa"
    output_csv_file = "/home/don/Documents/Temp/dev990/files_by_msa_summary.csv"

    # Process files and generate the CSV
    process_files_by_msa(base_directory, output_csv_file)

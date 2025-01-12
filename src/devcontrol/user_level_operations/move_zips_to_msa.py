import os
import shutil
from austin_zipcodes import get_austin_zips


def move_zips_to_msa(zips_by_city, base_folder):
    """
    Move zipcode folders from 'summary' to 'files_by_msa/austin' by city.

    Args:
        zips_by_city (dict): A dictionary mapping zip codes to city names.
        base_folder (str): The base folder containing the summary directory.
    """
    # Define paths
    summary_folder = os.path.join(base_folder, "summary")
    msa_folder = os.path.join(summary_folder, "files_by_msa/austin")

    # Create the base 'austin' directory
    os.makedirs(msa_folder, exist_ok=True)

    # Create directories for each city
    city_dirs = {}
    for city in set(zips_by_city.values()):
        city_dir = os.path.join(msa_folder, city)
        os.makedirs(city_dir, exist_ok=True)
        city_dirs[city] = city_dir

    # Move zipcode folders to corresponding city directories
    for zipcode, city in zips_by_city.items():
        zip_folder = os.path.join(summary_folder, str(zipcode))
        if os.path.exists(zip_folder):
            target_dir = city_dirs[city]
            shutil.move(zip_folder, target_dir)
            # print(f"Moved {zip_folder} to {target_dir}")

    print("Zipcode folders have been moved to the corresponding city directories.")


if __name__ == "__main__":
    # Define the base folder path
    base_directory = "/home/don/Documents/Temp/dev990"

    # Get Austin zip codes and their corresponding cities
    austin_zips, _ = get_austin_zips()

    # Execute the move function
    move_zips_to_msa(austin_zips, base_directory)

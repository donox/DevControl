import os
import shutil
from data_management.austin_zipcodes import get_austin_zips
def move_zips_to_msa(zips_by_city, all_zipcodes):
    """Move zipcode folders from all_zipcodes to output_directory by city"""
    base_folder_location = "/home/don/Documents/Temp/WW990/files_by_zip/"
    target_folder_location = "/home/don/Documents/Temp/WW990/files_by_msa"
    for zipcode, city in zips_by_city.items():
        dir_path = os.path.join(target_folder_location, city)
        os.makedirs(dir_path, exist_ok=True)
        zc = str(zipcode)
        source_path = os.path.join(base_folder_location, zc)
        if os.path.exists(source_path):
            shutil.move(source_path, dir_path)

if __name__ == "__main__":
    austin_zips, all_zips = get_austin_zips()
    move_zips_to_msa(austin_zips, all_zips)
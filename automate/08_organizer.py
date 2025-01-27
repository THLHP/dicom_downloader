import os
import shutil
import pydicom
import json
import re
from tqdm import tqdm

def get_dicom_metadata(filepath):
    try:
        dicom_file = pydicom.dcmread(filepath)
        patient_id = getattr(dicom_file, 'PatientID', 'Unknown')
        series_description = getattr(dicom_file, 'SeriesDescription', 'No Description').replace(' ', '_')
        # Replacing spaces with underscores and removing special characters makes the string more suitable for use in filenames
        series_uid = '.'.join(getattr(dicom_file, 'SeriesInstanceUID', '').split('.')[-2:])
        return patient_id, series_description, series_uid
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None, None, None

def organize_dicoms(input_dir, output_dir, test=False):
    pbar = tqdm(desc="Organizing DICOM files", unit="files")
    
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.lower().endswith('.dcm'):
                filepath = os.path.join(root, file)
                
                patient_id, series_description, series_uid = get_dicom_metadata(filepath)
                if not all([patient_id, series_description, series_uid]):
                    pbar.update(1)  # update progress bar even for skipped files
                    continue
                
                output_patient_dir = os.path.join(output_dir, str(patient_id))
                output_series_dir = os.path.join(output_patient_dir, f"{series_description}___{series_uid}")
                
                if test:
                    print(f"Would move {filepath} to {output_series_dir}")
                else:
                    os.makedirs(output_series_dir, exist_ok=True)
                    destination_file_path = os.path.join(output_series_dir, file)
                    
                    # If the destination file exists, delete the source file
                    if os.path.exists(destination_file_path):
                        print(f"Destination file already exists: {destination_file_path}. Deleting source file: {filepath}")
                        os.remove(filepath)
                    else:
                        shutil.move(filepath, output_series_dir)
                    
                pbar.update(1)  # update progress bar
                
    pbar.close()

if __name__ == "__main__":
    self_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(self_dir, "config.json")

    # Load credentials
    with open(config_path, 'r') as f:
        credentials = json.load(f)

    input_directory = credentials['path']['download']
    output_directory = credentials['path']['organized']
    test = False
    
    organize_dicoms(input_directory, output_directory, test=test)

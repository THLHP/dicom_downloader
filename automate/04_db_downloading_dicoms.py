import os
import json
import psycopg2
from psycopg2 import sql
from tqdm import tqdm
import time
from pynetdicom import AE, evt, build_role, debug_logger
from pynetdicom.sop_class import (
    PatientRootQueryRetrieveInformationModelFind,
    PatientRootQueryRetrieveInformationModelGet,
    StudyRootQueryRetrieveInformationModelFind,
    CTImageStorage,
    MRImageStorage,
    SecondaryCaptureImageStorage,
)
from pydicom.dataset import Dataset
from datetime import datetime

# debug_logger()

self_dir = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(self_dir, "config.json")

# Load credentials
with open(config_path, 'r') as f:
    credentials = json.load(f)

pacs_credentials = credentials['pacs']
db_credentials = credentials['database']

# Define the local storage directory
storage_dir = '/mnt/blockstorage/dicoms'

# PostgreSQL connection
conn = psycopg2.connect(
    dbname=db_credentials['dbname'],
    user=db_credentials['user'],
    password=db_credentials['password'],
    host=db_credentials['host'],
    port=db_credentials['port']
)
cur = conn.cursor()

# Define the PACS server details
PACS_IP = pacs_credentials['ip']
PACS_PORT = pacs_credentials['port']
PACS_AET = pacs_credentials['aet']
LOCAL_AET = pacs_credentials['local_aet']

def current_timestamp():
    """Return the current timestamp as a human-readable string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Define a handler for incoming C-STORE requests
def handle_store(event):
    """Handle a C-STORE request event."""
    ds = event.dataset
    ds.file_meta = event.file_meta
    
    # Define the filename and save the dataset
    patient_id = ds.PatientID
    series_instance_uid = ds.SeriesInstanceUID
    sop_instance_uid = ds.SOPInstanceUID
    series_name = ds.SeriesDescription if 'SeriesDescription' in ds else 'Unknown_Series'

    series_dir = os.path.join(storage_dir, patient_id, series_name)
    os.makedirs(series_dir, exist_ok=True)

    filename = os.path.join(series_dir, f"{sop_instance_uid}.dcm")
    ds.save_as(filename, write_like_original=False)
    # print(f"Saved file to {filename}")
    return 0x0000

handlers = [(evt.EVT_C_STORE, handle_store)]

# Initialize the Application Entity (AE)
ae = AE()
ae.acse_timeout = 3000
ae.dimse_timeout = 3000
ae.network_timeout = 3000
saved_files = set()

# Add requested presentation contexts for C-GET and storage of images
ae.add_requested_context(PatientRootQueryRetrieveInformationModelFind)
ae.add_requested_context(PatientRootQueryRetrieveInformationModelGet)
ae.add_requested_context(CTImageStorage)
ae.add_requested_context(MRImageStorage)
ae.add_requested_context(SecondaryCaptureImageStorage)

# Create an SCP/SCU Role Selection Negotiation item for each storage SOP class
storage_sop_classes = [CTImageStorage, MRImageStorage, SecondaryCaptureImageStorage]
roles = [build_role(sop_class, scp_role=True) for sop_class in storage_sop_classes]

# Function to download series data for a given series instance UID
def download_series(ae, pacs_address, pacs_port, called_aet, local_aet, patient_id, study_instance_uid, series_instance_uid, series_name, max_retries=20, wait_time=30):
    # Define the query dataset
    ds = Dataset()
    ds.QueryRetrieveLevel = 'SERIES'
    ds.PatientID = patient_id
    ds.StudyInstanceUID = study_instance_uid
    ds.SeriesInstanceUID = series_instance_uid

    retries = 0
    while retries < max_retries:
        try:
            # Perform the association with the PACS for C-GET
            assoc = ae.associate(pacs_address, pacs_port, ae_title=called_aet, ext_neg=roles, evt_handlers=handlers)

            if assoc.is_established:
                # Update the download_status to 'in_progress'
                update_download_status(series_instance_uid, 'in_progress')

                # Send the C-GET request
                responses = assoc.send_c_get(ds, PatientRootQueryRetrieveInformationModelGet)
                
                # Process the responses
                for (status, identifier) in responses:
                    if status and hasattr(status, 'Status') and status.Status in (0xFF00, 0xFF01):
                        # Identifier contains the matched dataset
                        pass
                    elif status and hasattr(status, 'Status') and status.Status == 0x0000:
                        # Completed
                        break
                    else:
                        print(f"Failed to retrieve series {series_instance_uid}: 0x{status.Status:04X}")

                # Release the association
                assoc.release()

                # Update the download_status to 'complete'
                update_download_status(series_instance_uid, 'complete')
                break  # Break out of the retry loop
            else:
                print(f"Association rejected, aborted or never connected for SeriesInstanceUID: {series_instance_uid}")

        except AttributeError as e:
            print(f"AttributeError: {e}")
            retries += 1
            if retries < max_retries:
                print(f"Retrying in {wait_time} seconds... (Attempt {retries}/{max_retries})")
                time.sleep(wait_time)
            else:
                print(f"Failed to download series {series_instance_uid} after {max_retries} attempts")
                break

def update_download_status(series_instance_uid, status):
    """Update the download status in the PostgreSQL database."""
    cur.execute("""
        UPDATE fieldsite.series
        SET download_status = %s, date_modified = CURRENT_TIMESTAMP
        WHERE seriesinstanceuid = %s
    """, (status, series_instance_uid))
    conn.commit()

# Main loop to download series
while True:
    # Query the database to get the next series to download
    cur.execute("""
        SELECT s.seriesinstanceuid, s.seriesdescription, p.patient_id, st.studyinstanceuid, s.numberofimages
        FROM fieldsite.series s
        JOIN fieldsite.studies st ON s.studyinstanceuid = st.studyinstanceuid
        JOIN fieldsite.patients p ON st.patient_id = p.patient_id
        WHERE s.download_status IS NULL OR s.download_status = ''
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    """)
    series_info = cur.fetchone()

    if not series_info:
        print("No more series to download. Exiting...")
        break

    series_instance_uid, series_name, patient_id, study_instance_uid, numimages = series_info
    print(f"{current_timestamp()} START: {patient_id} - {series_name} - {numimages}")
    
    download_series(ae, PACS_IP, PACS_PORT, PACS_AET, LOCAL_AET, patient_id, study_instance_uid, series_instance_uid, series_name)
    
    print(f"{current_timestamp()} END: {patient_id} - {series_name} - {numimages}")

# Close the database connection
cur.close()
conn.close()

print("Series data has been downloaded.")

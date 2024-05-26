import os
import json
import psycopg2
from psycopg2 import sql
from pydicom import dcmread
from tqdm import tqdm

# Load credentials
with open('config.json', 'r') as f:
    credentials = json.load(f)

db_credentials = credentials['database']

# Define the local storage directory
storage_dir = credentials['path']['downloads']

# PostgreSQL connection
conn = psycopg2.connect(
    dbname=db_credentials['dbname'],
    user=db_credentials['user'],
    password=db_credentials['password'],
    host=db_credentials['host'],
    port=db_credentials['port']
)
cur = conn.cursor()

def get_downloaded_images_count(series_dir):
    """Count the number of slices in the given series directory."""
    count = 0
    for filename in os.listdir(series_dir):
        if filename.endswith('.dcm'):
            filepath = os.path.join(series_dir, filename)
            ds = dcmread(filepath)
            if hasattr(ds, 'NumberOfFrames'):
                count += ds.NumberOfFrames
            else:
                count += 1
    return count

def update_validation_status(patient_id, series_name, status):
    """Update the validation status in the PostgreSQL database."""
    cur.execute("""
        UPDATE fieldsite.series
        SET validation = %s, date_modified = CURRENT_TIMESTAMP
        WHERE seriesdescription = %s AND studyid IN (
            SELECT studyid FROM fieldsite.studies
            WHERE patient_id = %s
        )
    """, (status, series_name, patient_id))
    conn.commit()

# Query all series with download status 'complete' grouped by seriesdescription
cur.execute("""
    SELECT p.patient_id, s.seriesdescription, SUM(s.numberofimages) AS total_numberofimages
    FROM fieldsite.series s
    JOIN fieldsite.studies st ON s.studyid = st.studyid
    JOIN fieldsite.patients p ON st.patient_id = p.patient_id
    WHERE s.download_status = 'complete'
    GROUP BY p.patient_id, s.seriesdescription
""")
series_list = cur.fetchall()

missing_slices_report = []

# Iterate through each series and compare the downloaded images count with the expected number of images
with tqdm(total=len(series_list), desc="Checking series", unit="series") as pbar:
    for series_info in series_list:
        patient_id, series_name, expected_num_images = series_info
        series_dir = os.path.join(storage_dir, patient_id, series_name)

        pbar.set_description(f"Checking {series_name}")

        if not os.path.exists(series_dir):
            print(f"Directory does not exist for series {series_name}: {series_dir}")
            update_validation_status(patient_id, series_name, 'failed')
            pbar.update(1)
            continue

        downloaded_num_images = get_downloaded_images_count(series_dir)

        if downloaded_num_images != expected_num_images:
            missing_slices_report.append({
                "patient_id": patient_id,
                "series_name": series_name,
                "expected_num_images": expected_num_images,
                "downloaded_num_images": downloaded_num_images
            })
            update_validation_status(patient_id, series_name, 'failed')
        else:
            update_validation_status(patient_id, series_name, 'complete')

        pbar.update(1)

# Print the report
print("Missing Slices Report:")
for report in missing_slices_report:
    print(f"PatientID: {report['patient_id']}, SeriesName: {report['series_name']}, "
          f"Expected: {report['expected_num_images']}, Downloaded: {report['downloaded_num_images']}")

# Close the database connection
cur.close()
conn.close()

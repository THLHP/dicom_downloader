import os
import psycopg2
import pandas as pd
from datetime import datetime
import glob
import json
# Using the requests library:
import requests

try:
    requests.get("https://hc-ping.com/hLLDZXOBn0N0ABea5bVJKQ/dicom_inventory_generator/start", timeout=10)
except requests.RequestException as e:
    # Log ping failure here...
    print("Ping failed: %s" % e)

script_dir = os.path.dirname(os.path.realpath(__file__))
credentials_path = os.path.join(script_dir, 'credentials.json')

# Load credentials from 'credentials.json'
with open(credentials_path, 'r') as f:
    credentials = json.load(f)

db_credentials = credentials['database']

# PostgreSQL connection
conn = psycopg2.connect(
    dbname=db_credentials['dbname'],
    user=db_credentials['user'],
    password=db_credentials['password'],
    host=db_credentials['host'],
    port=db_credentials['port']
)
cur = conn.cursor()

# Directory to save the CSV file
output_dir = credentials['path']['asu_path']
os.makedirs(output_dir, exist_ok=True)

# Remove any previously created CSV files
for filepath in glob.glob(os.path.join(output_dir, 'dicom_inventory_*.csv')):
    os.remove(filepath)

# Query to get all series
query = """
    SELECT
    "fieldsite"."patients"."patient_id" AS "patient_id",
    "fieldsite"."patients"."patient_name" AS "patient_name",
    "fieldsite"."patients"."patient_sex" AS "patient_sex",
    "Studies"."studyid" AS "Studies__studyid",
    "Studies"."patient_id" AS "Studies__patient_id",
    "Studies"."study_datetime" AS "Studies__study_datetime",
    "Studies"."studyinstanceuid" AS "Studies__studyinstanceuid",
    "Studies"."accession_number" AS "Studies__accession_number",
    "Series"."series_id" AS "Series__series_id",
    "Series"."studyid" AS "Series__studyid",
    "Series"."seriesinstanceuid" AS "Series__seriesinstanceuid",
    "Series"."series_datetime" AS "Series__series_datetime",
    "Series"."seriesnumber" AS "Series__seriesnumber",
    "Series"."modality" AS "Series__modality",
    "Series"."institutionname" AS "Series__institutionname",
    "Series"."institutionaldepartmentname" AS "Series__institutionaldepartmentname",
    "Series"."seriesdescription" AS "Series__seriesdescription",
    "Series"."bodypartexamined" AS "Series__bodypartexamined",
    "Series"."numberofimages" AS "Series__numberofimages"
    FROM
    "fieldsite"."patients"
    
    LEFT JOIN "fieldsite"."studies" AS "Studies" ON "fieldsite"."patients"."patient_id" = "Studies"."patient_id"
    LEFT JOIN "fieldsite"."series" AS "Series" ON "Studies"."studyid" = "Series"."studyid"
    LIMIT
    1048575
"""

# Fetch all data
cur.execute(query)
data = cur.fetchall()

# Column names
columns = ["patient_id","patient_name","patient_sex","Studies__studyid","Studies__patient_id","Studies__study_datetime","Studies__studyinstanceuid","Studies__accession_number","Series__series_id","Series__studyid","Series__seriesinstanceuid","Series__series_datetime","Series__seriesnumber","Series__modality","Series__institutionname","Series__institutionaldepartmentname","Series__seriesdescription","Series__bodypartexamined","Series__numberofimages"]

# Create a DataFrame
df = pd.DataFrame(data, columns=columns)


# Check if the DataFrame is not empty
if not df.empty:
    # Generate a filename with the current date
    timestamp = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    filename = os.path.join(output_dir, f'dicom_inventory_{timestamp}.csv')

    # Save to CSV
    df.to_csv(filename, index=False)

    print(f'Series list saved to {filename}')

    # Remove any previously created CSV files
    for filepath in glob.glob(os.path.join(output_dir, 'dicom_inventory_*.csv')):
        if filepath != filename:
            os.remove(filepath)
            print(f'Removed old CSV file: {filepath}')
else:
    print("No data retrieved. No new CSV file created.")

# Close the database connection
cur.close()
conn.close()

try:
    requests.get("https://hc-ping.com/hLLDZXOBn0N0ABea5bVJKQ/dicom_inventory_generator", timeout=10)
except requests.RequestException as e:
    # Log ping failure here...
    print("Ping failed: %s" % e)

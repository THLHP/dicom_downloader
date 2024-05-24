import json
import psycopg2
from psycopg2 import sql, extras
from tqdm import tqdm
from pynetdicom import AE
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind
from pydicom.dataset import Dataset
import os

self_dir = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(self_dir, "config.json")

# Load credentials
with open(config_path, 'r') as f:
    credentials = json.load(f)

pacs_credentials = credentials['pacs']
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

# Query all studies from the studies table
cur.execute("SELECT studyid, studyinstanceuid FROM fieldsite.studies")
studies = cur.fetchall()

study_map = {study[1]: study[0] for study in studies}  # Map studyinstanceuid to studyid

# Initialize the Application Entity (AE)
ae = AE()

# Add requested presentation context for C-FIND operation
ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)

# Define the PACS server details
PACS_IP = pacs_credentials['ip']
PACS_PORT = pacs_credentials['port']
PACS_AET = pacs_credentials['aet']
LOCAL_AET = pacs_credentials['local_aet']

# List to hold series data for batch insertion
series_data = []

for studyinstanceuid, studyid in tqdm(study_map.items(), desc="Querying series for studies"):
    # Define the query dataset
    ds = Dataset()
    ds.QueryRetrieveLevel = 'SERIES'
    ds.StudyInstanceUID = studyinstanceuid
    ds.SeriesInstanceUID = ''
    ds.SeriesDate = ''
    ds.SeriesTime = ''
    ds.SeriesNumber = ''
    ds.Modality = ''
    ds.InstitutionName = ''
    ds.InstitutionalDepartmentName = ''
    ds.SeriesDescription = ''
    ds.BodyPartExamined = ''
    ds.NumberOfSeriesRelatedInstances = ''

    # Perform the association with the PACS
    assoc = ae.associate(PACS_IP, PACS_PORT, ae_title=PACS_AET)

    if assoc.is_established:
        # Send the C-FIND request
        responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)
        
        for (status, identifier) in responses:
            if status.Status in (0xFF00, 0xFF01):
                series_instance_uid = identifier.SeriesInstanceUID if 'SeriesInstanceUID' in identifier else None
                series_number = identifier.SeriesNumber if 'SeriesNumber' in identifier else None
                modality = identifier.Modality if 'Modality' in identifier else None
                institution_name = identifier.InstitutionName if 'InstitutionName' in identifier else None
                institutional_department_name = identifier.InstitutionalDepartmentName if 'InstitutionalDepartmentName' in identifier else None
                series_description = identifier.SeriesDescription if 'SeriesDescription' in identifier else None
                body_part_examined = identifier.BodyPartExamined if 'BodyPartExamined' in identifier else None
                number_of_images = identifier.NumberOfSeriesRelatedInstances if 'NumberOfSeriesRelatedInstances' in identifier else None
                series_date = identifier.SeriesDate if 'SeriesDate' in identifier else None
                series_time = identifier.SeriesTime if 'SeriesTime' in identifier else None

                if series_date and series_time:
                    series_datetime = f"{series_date} {series_time}"
                elif series_date:
                    series_datetime = series_date
                elif series_time:
                    series_datetime = series_time
                else:
                    series_datetime = None

                if series_instance_uid:
                    series_data.append(
                        (studyid, series_instance_uid, series_datetime, series_number, modality,
                         institution_name, institutional_department_name, series_description,
                         body_part_examined, number_of_images)
                    )

        # Release the association
        assoc.release()
    else:
        print(f"Association rejected, aborted or never connected for StudyInstanceUID: {studyinstanceuid}")

# Insert series data in batch
if series_data:
    insert_query = """
        INSERT INTO fieldsite.series (studyid, seriesinstanceuid, series_datetime, seriesnumber, modality,
                                      institutionname, institutionaldepartmentname, seriesdescription,
                                      bodypartexamined, numberofimages)
        VALUES %s
        ON CONFLICT (seriesinstanceuid) DO UPDATE
        SET studyid = EXCLUDED.studyid,
            series_datetime = EXCLUDED.series_datetime,
            seriesnumber = EXCLUDED.seriesnumber,
            modality = EXCLUDED.modality,
            institutionname = EXCLUDED.institutionname,
            institutionaldepartmentname = EXCLUDED.institutionaldepartmentname,
            seriesdescription = EXCLUDED.seriesdescription,
            bodypartexamined = EXCLUDED.bodypartexamined,
            numberofimages = EXCLUDED.numberofimages,
            date_modified = CURRENT_TIMESTAMP;
    """
    extras.execute_values(cur, insert_query, series_data)
    conn.commit()

# Close the database connection
cur.close()
conn.close()

print("Series data has been populated.")

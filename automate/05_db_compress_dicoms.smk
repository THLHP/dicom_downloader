import os
import json
import psycopg2

configfile: "config.json"

def get_patients_with_complete_downloads():
    
    db_credentials = config['database']
    
    # PostgreSQL connection
    conn = psycopg2.connect(
        dbname=db_credentials['dbname'],
        user=db_credentials['user'],
        password=db_credentials['password'],
        host=db_credentials['host'],
        port=db_credentials['port']
    )
    cur = conn.cursor()
    
    # Query to get patient_ids where download percentage is 100%
    query = """
    SELECT patient_id
    FROM fieldsite.series_download_status
    WHERE percentage_downloaded = 100.0;
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    # Extract patient_ids from the results
    patient_ids = [row[0] for row in results]
    
    # Close the database connection
    cur.close()
    conn.close()
    
    return patient_ids

rule all:
    input:
        expand(config['path']['compressed'] + "{patient_id}.zip", patient_id = get_patients_with_complete_downloads())

rule zip_directories:
    input:
        (config['path']['download']  + "{input}")
    output:
        config['path']['compressed'] + "{input}.zip"
    resources:
        cpus=1
    shell:
        'zip -r "{output}" "{input}" > /dev/null'

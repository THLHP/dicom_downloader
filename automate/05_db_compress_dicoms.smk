import os
import json
import psycopg2

configfile: "config.json"

def zip_input(wildcard):
    return os.path.join(config['path']['download'], wildcard.input.split(config['delimiter'])[0])

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
    
    cur.execute("""
        SELECT patient_id, total_series
        FROM fieldsite.patients_with_complete_downloads;
    """)

    return_all = cur.fetchall()

    # Close the database connection
    cur.close()
    conn.close()
    
    return return_all

rule all:
    input:
        expand(config['path']['compressed'] + "{patient_id[0]}{delimiter}{patient_id[1]}.zip", 
        patient_id = get_patients_with_complete_downloads(), delimiter=config['delimiter'])

rule zip_directories:
    input:
        zip_input
    output:
        config['path']['compressed'] + "{input}.zip"
    resources:
        cpus=1
    shell:
        'zip -r "{output}" "{input}" > /dev/null'

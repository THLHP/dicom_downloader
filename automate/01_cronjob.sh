#!/bin/bash

# A sample crontab entry. Note the curl call appended after the command.
# FIXME: replace "/your/command.sh" below with the correct command!
curl -fsS -m 10 --retry 5 -o /dev/null https://hc-ping.com/hLLDZXOBn0N0ABea5bVJKQ/pacs_to_thlhp_db_postgresql/start

echo "Inserting new patients to DB"
/root/miniconda3/envs/sql/bin/python /root/dicom_downloader/automate/01_db_insert_patients.py

echo "Inserting new studies to DB"
/root/miniconda3/envs/sql/bin/python /root/dicom_downloader/automate/02_db_insert_studies.py

echo "Inserting new series to DB"
/root/miniconda3/envs/sql/bin/python /root/dicom_downloader/automate/03_db_insert_series.py

curl -fsS -m 10 --retry 5 -o /dev/null https://hc-ping.com/hLLDZXOBn0N0ABea5bVJKQ/pacs_to_thlhp_db_postgresql/$?

echo "All Done"
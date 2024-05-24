# DICOM Downloader
This repository contains 5 scripts that orchestrates the download of DICOMs from a given PACS to a local server using the PACS proctols (C-Find, C-Move). The scripts synchrnoize their states using a backend postgresql database though it can be modified to use a simple CSV file. The scripts are separated due to their reliance on different resources (cpu, upload or download) and thus can all be executed simultaneously to operate in an assembly line fashion. 

## Code overview
### Configuration
* `schema.sql` - Schema required for the database. Also contains optional triggers for the database to automatically update the `date_modified` columns.
* `config-sample.json` - This file contains credentials for PACS and PostgresQL as well as the paths for download. This file needs to be filled in with valid credentials and renamed to `config.json` before starting any other scripts.

### Scripts
* `automate/01_db_insert_patients.py` - Queries PACS for all patients and any patient IDs that match a predetermined criteria get added to the PostgresQL database. The criteria is determined by the function `detect_thlhp_patient()`
* `automate/02_db_insert_studies.py` - Fetches all patient_ids from the database, queries the PACS for all studies associated with the patient_ids and adds them to the database. 
* `automate/03_db_insert_series.py` - Fetches all study_ids from the database, queries all series associated with them and populates the database with them.
* `automate/04_db_downloading_dicoms.py` - Queries the database for any series that has not been marked as `complete` in the column `download_status` and downloads them. Multiple instances of the script can be executed to download multiple DICOMs in parallel.
* `automate/05_db_compress_dicoms.smk` - [Snakemake](https://snakemake.github.io/) script that queries the database for all patients where all of their series have been downloaded and then creates a compressed file containing all the series for that patient.

## Automation
Use `crontab -e` to schedule the scripts. Sample bash driver files are provided in the `automate` directory with optional heartbeat integeration. 

## Current implementation notes
* The internet connection in Bolivia resets every day at 3am local time which the downloader script can now handle. If not handle, it causes the downloading script to go in an infinite loop. 
* The PACS can support ~700 requests a minute, anything beyond that causes all connections to be reset. 

## Future todo
* Make the snakemake script recreate the zip file if new series are detected for a patient.
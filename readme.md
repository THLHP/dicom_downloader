![pacs_to_thlhp_db_postgresql](https://healthchecks.io/b/2/ece36f1b-0563-47ad-8911-b93237e10d87.svg)
![dicoms_pacs_to_chile](https://healthchecks.io/b/2/f2ad5eba-2d27-49d1-b3e5-091f8b97d0a8.svg)
![smk-compressor-chile-dicoms](https://healthchecks.io/b/2/854994fc-24f0-4554-8e88-74ab3b7bc42e.svg)
![dicoms_chile_to_asu](https://healthchecks.io/b/2/7d4cdf31-c1df-4a9a-b556-07fc1b88b08e.svg)
![dicom_inventory_generator](https://healthchecks.io/b/2/204fb71d-2787-4fcd-983e-a6f983bf8922.svg)

# DICOM Downloader
This repository contains 5 scripts that orchestrates the download of DICOMs from a given PACS to a local server using the PACS proctols (C-Find, C-Move). The scripts synchrnoize their states using a backend postgresql database though it can be modified to use a simple CSV file. The scripts are separated due to their reliance on different resources (cpu, upload or download) and thus can all be executed simultaneously to operate in an assembly line fashion. 

The output zip files are named as `<patient-id>_<delimiter><count of series>`. 
* `patient-id` is defined in the DICOM file itself
* `delimiter` is defined in the `config.json`, *(default is __seriesCount)*
* `count of series` is the number of series in the file. 

This means if the patient `AA-BB-CC` currently has 7 series then the zip file would have the name `AA-BB-CC__seriesCount7.zip`. If this patient gets another series in future then a new zip file named `AA-BB-CC__seriesCount8.zip` would automatically get created.

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
* `automate/06_validate_slices.py` - Optional script that validates if the total number of downloaded slices match the number of slices for each series in the database.
* `automate/07_dicom_inventory_generator.py` - Creates a CSV file of all series, studies, patients in the database. 

## Automation

![CT Scans flow diagram](images/automation_diagram.drawio.png)

Use `crontab -e` to schedule the scripts. Sample bash driver files are provided in the `automate` directory with optional heartbeat integeration. The badges at top of this readme provide current status of the automations. 
* `pacs_to_thlhp_db_postgresql` - Status of scripts 01-03
* `dicoms_pacs_to_chile` - Status of script 04
* `smk-compressor-chile-dicoms` - Status of script 05
* `dicoms_chile_to_asu` - Status of rsync script that copies all compressed files from Chile to ASU storage and makes it available on Globus. 
* `dicom_inventory_generator` - Status of script 07. Currently runs on ASU servers and the report is created in the same directory as the DICOMs. 

## Current implementation notes
* The internet connection in Bolivia resets every day at 3am local time which the downloader script can now handle. If not handle, it causes the downloading script to go in an infinite loop. 
* The PACS can support ~700 requests a minute, anything beyond that causes all connections to be reset. 

## Future todo
* ~~Make the snakemake script recreate the zip file if new series are detected for a patient.~~ Completed.
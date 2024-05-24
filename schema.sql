-- Create patients table
CREATE TABLE fieldsite.patients (
    patient_id VARCHAR PRIMARY KEY,
    patient_name VARCHAR,
    patient_sex VARCHAR,
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create studies table
CREATE TABLE IF NOT EXISTS fieldsite.studies (
    studyid VARCHAR(64) PRIMARY KEY,
    patient_id VARCHAR(64) REFERENCES fieldsite.patients(patient_id) ON DELETE CASCADE,
    study_datetime TIMESTAMP,
    studyinstanceuid VARCHAR(64),
    accession_number VARCHAR(64),
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create series table
CREATE TABLE IF NOT EXISTS fieldsite.series (
    series_id SERIAL PRIMARY KEY,
    studyid VARCHAR(64) REFERENCES fieldsite.studies(studyid) ON DELETE CASCADE,
    seriesinstanceuid VARCHAR(64) UNIQUE,
    series_datetime TIMESTAMP,
    seriesnumber INTEGER,
    modality VARCHAR(16),
    institutionname VARCHAR(255),
    institutionaldepartmentname VARCHAR(255),
    seriesdescription VARCHAR(255),
    bodypartexamined VARCHAR(64),
    numberofimages INTEGER,
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE fieldsite.series
ADD COLUMN download_status VARCHAR(16);

-- Trigger to update date_modified column
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.date_modified = CURRENT_TIMESTAMP;
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for patients table
CREATE TRIGGER update_patients_modtime
BEFORE UPDATE ON fieldsite.patients
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Trigger for studies table
CREATE TRIGGER update_studies_modtime
BEFORE UPDATE ON fieldsite.studies
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Trigger for series table
CREATE TRIGGER update_series_modtime
BEFORE UPDATE ON fieldsite.series
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Create view for determining if all downloads are complete for a patient
CREATE VIEW fieldsite.series_download_status AS
SELECT 
    p.patient_id,
    COUNT(s.series_id) AS total_series,
    COUNT(CASE WHEN s.download_status = 'complete' THEN 1 END) AS series_downloaded,
    ROUND(
        COUNT(CASE WHEN s.download_status = 'complete' THEN 1 END)::numeric / COUNT(s.series_id) * 100, 2
    ) AS percentage_downloaded
FROM 
    fieldsite.patients p
JOIN 
    fieldsite.studies st ON p.patient_id = st.patient_id
JOIN 
    fieldsite.series s ON st.studyid = s.studyid
GROUP BY 
    p.patient_id;

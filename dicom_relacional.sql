/* DICOM_RDB_STORING: */

CREATE TABLE patient (
    pk SERIAL PRIMARY KEY,
    patient_id VARCHAR(64) UNIQUE,
    patient_name VARCHAR(64),
    patient_sex VARCHAR(16),
    patient_age VARCHAR(4),
    patient_weight NUMERIC(13,2),
    patient_birth_date DATE
);

CREATE TABLE study (
    pk SERIAL PRIMARY KEY,
    study_instance_uid VARCHAR(64) UNIQUE,
    study_description VARCHAR(64),
    study_date DATE,
    study_time TIMESTAMP,
    study_id VARCHAR(16),
    patient_fk INTEGER
);

CREATE TABLE series (
    pk SERIAL PRIMARY KEY,
    series_instance_uid VARCHAR(64) UNIQUE,
    series_description VARCHAR(64),
    series_date DATE,
    series_time TIMESTAMP,
    series_number INTEGER,
    study_fk INTEGER,
    body_part_examined VARCHAR(16),
    requested_procedure_description VARCHAR(64),
    modality VARCHAR(16)
);

CREATE TABLE image (
    pk SERIAL PRIMARY KEY,
    sop_instance_uid VARCHAR(64) UNIQUE,
    series_fk INTEGER,
    sop_class_uid VARCHAR(64),
    equipment_fk INTEGER,
    acquisition_date DATE,
    acquisition_time TIMESTAMP,
    kvp NUMERIC(13,2),
    exposure_time INTEGER,
    exposure INTEGER,
    x_ray_tube_current INTEGER,
    image_type VARCHAR(16)
);

CREATE TABLE equipment (
    pk SERIAL PRIMARY KEY,
    software_version VARCHAR(64),
    last_calibration_date DATE,
    last_calibration_time TIMESTAMP,
    station_name VARCHAR(16),
    manufacturer_model_name VARCHAR(64),
    manufacturer_fk INTEGER
);

CREATE TABLE manufacturer (
    pk SERIAL PRIMARY KEY,
    name VARCHAR(64)
);

ALTER TABLE study ADD CONSTRAINT FK_study_2
    FOREIGN KEY (patient_fk)
    REFERENCES patient (pk);

ALTER TABLE series ADD CONSTRAINT FK_series_2
    FOREIGN KEY (study_fk)
    REFERENCES study (pk);

ALTER TABLE image ADD CONSTRAINT FK_image_2
    FOREIGN KEY (series_fk)
    REFERENCES series (pk);

ALTER TABLE image ADD CONSTRAINT FK_image_3
    FOREIGN KEY (equipment_fk)
    REFERENCES equipment (pk);

ALTER TABLE equipment ADD CONSTRAINT FK_equipment_1
    FOREIGN KEY (manufacturer_fk)
    REFERENCES manufacturer (pk);

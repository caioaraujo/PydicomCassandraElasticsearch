import datetime
import os
import sys
from decimal import Decimal

import psycopg2
import pydicom
from cassandra.cluster import Cluster


def __validate_dir(path):
    directory_does_not_exist_exception = Exception("Directory does not exists. Please type a valid path")

    if not os.path.isdir(path):
        raise directory_does_not_exist_exception

    return path

def __insert_postgres(dicom_data):
    patient_pk = None
    series_fk = None
    equipment_fk = None
    manufacturer_fk = None
    study_fk = None
    patient_birth_date=None
    patient_weight=None

    if dicom_data.get('PatientBirthDate'):
        patient_birth_date = datetime.datetime.strptime(dicom_data.get('PatientBirthDate'), '%Y%m%d').date()

    if dicom_data.get('PatientWeight'):
        patient_weight=Decimal(dicom_data.get('PatientWeight'))


    patient = dict(
        id=dicom_data.get('PatientID'),
        name=str(dicom_data.get('PatientName')),
        sex=dicom_data.get('PatientSex'),
        age=dicom_data.get('PatientAge'),
        weight=patient_weight,
        birth_date=patient_birth_date
    )

    patient_pk = get_pk("""select pk from patient where patient_id = %s""", patient.get('id'))

    if not patient_pk:
        patient_pk = __insert_patient(patient)

    study = dict(
        instance_uid=dicom_data.get('StudyInstanceUID'),
        description=dicom_data.get('StudyDescription'),
        date=dicom_data.get('StudyDate'),
        time=dicom_data.get('StudyTime'),
        id=dicom_data.get('StudyID'),
        fk=patient_pk
    )

    series = dict(
        series_instance_uid=dicom_data.get('SeriesInstanceUID'),
        series_description=dicom_data.get('SeriesDescription'),
        series_date=dicom_data.get('SeriesDate'),
        series_time=dicom_data.get('SeriesTime'),
        series_number=dicom_data.get('SeriesNumber'),
        body_part_examined=dicom_data.get('BodyPartExamined'),
        requested_procedure_description=dicom_data.get('RequestedProcedureDescription'),
        modality=dicom_data.get('Modality'),
        study_fk=study_fk
    )

    manufacturer = dict(
        name=dicom_data.get('Manufacturer')
    )

    equipment = dict(
        software_version=dicom_data.get('SoftwareVersions'),
        last_calibration_date=dicom_data.get('DateOfLastCalibration'),
        last_calibration_time=dicom_data.get('TimeOfLastCalibration'),
        manufacturer_model_name=dicom_data.get('ManufacturerModelName'),
        station_name=dicom_data.get('StationName'),
        manufacturer_fk=manufacturer_fk
    )

    image = dict(
        sop_instance_uid=dicom_data.get('SOPInstanceUID'),
        sop_class_uid=dicom_data.get('SOPClassUID'),
        acquisition_date=dicom_data.get('AcquisitionDate'),
        acquisition_time=dicom_data.get('AcquisitionTime'),
        kvp=dicom_data.get('KVP'),
        exposure_time=dicom_data.get('ExposureTime'),
        exposure=dicom_data.get('Exposure'),
        x_ray_tube_current=dicom_data.get('XRayTubeCurrent'),
        image_type=dicom_data.get('ImageType'),
        series_fk=series_fk,
        equipment_fk=equipment_fk
    )


def __insert_patient(patient):
    dml = """ 
    INSERT INTO PATIENT (PATIENT_ID, PATIENT_NAME, PATIENT_SEX, PATIENT_WEIGHT, PATIENT_BIRTH_DATE) 
    VALUES (%s, %s, %s, %s, %s) RETURNING PK;
    """

    patient_pk = None
    try:
        conn = __connect_postgres()
        cur = conn.cursor()
        cur.execute(dml, (patient.get('id'), patient.get('name'), patient.get('sex'), patient.get('weight'), patient.get('birth_date')))
        patient_pk = cur.fetchone()[0]
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return patient_pk


def get_pk(sql, uid):
    try:
        conn = __connect_postgres()
        cur = conn.cursor()
        cur.execute(sql, (uid,))
        print("The number of items: ", cur.rowcount)
        row = cur.fetchone()
        pk = None

        if row is not None:
            pk = row[0]

        cur.close()
        return pk

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def __connect_postgres():
    return psycopg2.connect(dbname="dicomrdb", user="postgres", password="postgres")

if __name__ == '__main__':

    root = __validate_dir(sys.argv[1])

    # Connect to Cassandra
    #cluster = Cluster()


    #session = cluster.connect('dicom')


    for path, subdirs, files in os.walk(root):
        for name in files:
            file = os.path.join(path, name)

            file_extension = name.split('.')[-1]
            if file_extension.lower() != 'dcm':
                print('Ignoring file:', file)
                continue

            print('Working in file:', file)

            dicom_dataset = pydicom.dcmread(file)

            __insert_postgres(dicom_dataset)


import base64
import datetime
import os
import sys
from decimal import Decimal

import psycopg2
import pydicom
from cassandra.cluster import Cluster
from elasticsearch_dsl import Document, Text
from elasticsearch_dsl.connections import connections


class DicomDoc(Document):
    data_exame = Text(analyzer='snowball')
    nome_paciente = Text(analyzer='snowball')
    descricao_estudo = Text(analyzer='snowball')
    descricao_serie = Text(analyzer='snowball')
    data_nasc_paciente = Text(analyzer='snowball')
    especialidade_exame = Text(analyzer='snowball')

    class Index:
        name = 'dicom'
        settings = {
            "number_of_shards": 2,
        }

    def save(self, **kwargs):
        self.lines = len(self.body.split())
        return super(DicomDoc, self).save(**kwargs)


def __validate_dir(path):
    directory_does_not_exist_exception = Exception("Directory does not exists. Please type a valid path")

    if not os.path.isdir(path):
        raise directory_does_not_exist_exception

    return path

def __insert_postgres(dicom_data):
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

    study_date = None
    if dicom_data.get('StudyDate'):
        study_date = datetime.datetime.strptime(dicom_data.get('StudyDate'), '%Y%m%d').date()

    study_time = None
    #if dicom_data.get('StudyTime'):
    #    study_time = datetime.datetime.strptime(dicom_data.get('StudyTime'), '%H%M%S').hour()

    study = dict(
        instance_uid=dicom_data.get('StudyInstanceUID'),
        description=dicom_data.get('StudyDescription'),
        date=study_date,
        time=study_time,
        id=dicom_data.get('StudyID'),
        patient_fk=patient_pk
    )

    study_pk = get_pk("""select pk from study where study_instance_uid = %s""", study.get('instance_uid'))

    if not study_pk:
        study_pk = __insert_study(study)

    series_date = None
    if dicom_data.get('SeriesDate'):
        series_date = datetime.datetime.strptime(dicom_data.get('SeriesDate'), '%Y%m%d').date()

    series = dict(
        instance_uid=dicom_data.get('SeriesInstanceUID'),
        description=dicom_data.get('SeriesDescription'),
        date=series_date,
        time=dicom_data.get('SeriesTime'),
        number=dicom_data.get('SeriesNumber'),
        body_part_examined=dicom_data.get('BodyPartExamined'),
        requested_procedure_description=dicom_data.get('RequestedProcedureDescription'),
        modality=dicom_data.get('Modality'),
        study_fk=study_pk
    )

    series_pk = get_pk("""select pk from series where series_instance_uid = %s""", series.get('instance_uid'))

    if not series_pk:
        series_pk = __insert_series(series)

    manufacturer = dict(
        name=dicom_data.get('Manufacturer')
    )

    equipment = dict(
        software_version=dicom_data.get('SoftwareVersions'),
        last_calibration_date=dicom_data.get('DateOfLastCalibration'),
        last_calibration_time=dicom_data.get('TimeOfLastCalibration'),
        manufacturer_model_name=dicom_data.get('ManufacturerModelName'),
        station_name=dicom_data.get('StationName'),
        manufacturer_fk=0
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
        series_fk=0,
        equipment_fk=0
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


def __insert_study(study):
    dml = """ 
    INSERT INTO STUDY (STUDY_INSTANCE_UID, STUDY_DESCRIPTION, STUDY_DATE, 
    --STUDY_TIME, 
    STUDY_ID, PATIENT_FK) 
    VALUES (%s, %s, %s, %s, %s) RETURNING PK;
    """

    study_pk = None
    try:
        conn = __connect_postgres()
        cur = conn.cursor()
        cur.execute(dml, (study.get('instance_uid'), study.get('description'), study.get('date'),
                          #study.get('time'),
                          study.get('id'), study.get('patient_fk')))
        study_pk = cur.fetchone()[0]
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return study_pk


def __insert_series(series):
    dml = """ 
    INSERT INTO SERIES (SERIES_INSTANCE_UID, SERIES_DESCRIPTION, SERIES_DATE, 
    --SERIES_TIME
    SERIES_NUMBER, STUDY_FK, BODY_PART_EXAMINED, REQUESTED_PROCEDURE_DESCRIPTION, MODALITY) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING PK;
    """

    series_pk = None
    try:
        conn = __connect_postgres()
        cur = conn.cursor()
        cur.execute(dml, (series.get('instance_uid'), series.get('description'), series.get('date'),
                          series.get('number'), series.get('study_fk'), series.get('body_part_examined'),
                          series.get('requested_procedure_description'), series.get('modality')))
        series_pk = cur.fetchone()[0]
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return series_pk


def get_pk(sql, uid):
    try:
        conn = __connect_postgres()
        cur = conn.cursor()
        cur.execute(sql, (uid,))
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

def __extract_dataset_to_dict(dicom_dict, dicom_dataset, file_data):

    if dicom_dataset.SeriesInstanceUID not in dicom_dict:
        dicom_dict[dicom_dataset.SeriesInstanceUID] = []

    data_instance = dict({'sop_instance_uid': dicom_dataset.SOPInstanceUID, 'data': file_data})
    dicom_dict.get(dicom_dataset.SeriesInstanceUID).append(data_instance)

    return dicom_dict[dicom_dataset.SeriesInstanceUID]


def __define_column_family(dicom_dataset):

    if not dicom_dataset:
        return

    rowkey = dicom_dataset.StudyInstanceUID
    column_family = 'series:{}'.format(dicom_dataset.SeriesInstanceUID)

    return rowkey, column_family


def __set_patient(patient_dict, dicom_dataset, image_bytes):
    id_paciente = str(dicom_dataset.PatientID)

    id_image = str(dicom_dataset.SOPInstanceUID)

    image_data = {id_image: str(image_bytes)}

    patient = patient_dict.get(id_paciente)

    if not patient:
        patient_dict[id_paciente] = image_data

    else:
        patient[id_image] = str(image_bytes)


def __set_studies(study_dict, dicom_dataset, rowkey):
    study_uid = str(rowkey)

    study = study_dict.get(study_uid)

    series_uid = str(dicom_dataset.SeriesInstanceUID)

    id_image = str(dicom_dataset.SOPInstanceUID)

    if not study:
        study_dict[study_uid] = {series_uid: [id_image]}

    else:
        series = study.get(series_uid)
        if series:
            series.append(id_image)
        else:
            study[series_uid] = [id_image]


def __set_images(image_dict, dicom_dataset, image_bytes):
    id_image = str(dicom_dataset.SOPInstanceUID)

    image = image_dict.get(id_image)

    if not image:
        image_dict[id_image] = str(image_bytes)


def __insert_into_cassandra(patient_dict, study_dict, image_dict):

    # Insere pacientes
    for id_paciente, images in patient_dict.items():

        session.execute(
            """
            INSERT INTO patient (patient_id, image)
            VALUES (%(id_paciente)s, %(image_data)s)
            """,
            {'id_paciente': id_paciente, 'image_data': images}
        )

        print(f'inseriu paciente {id_paciente}')

    # Insere studies
    for id_study, series in study_dict.items():
        session.execute(
            """
            INSERT INTO study (study_uid, series)
            VALUES (%(id_study)s, %(series)s)
            """,
            {'id_study': id_study, 'series': series}
        )

        print(f'inseriu study {id_study}')

    # Insere imagens
    for id_image, image_bytes in image_dict.items():
        session.execute(
            """
            INSERT INTO image (sop_instance_uid, image_bytes)
            VALUES (%(id_image)s, %(image_bytes)s)
            """,
            {'id_image': id_image, 'image_bytes': image_bytes}
        )

        print(f'inseriu imagem {id_image}')


def __insert_into_elasticsearch(rowkey, dicom_dataset):
    connections.create_connection(hosts=['localhost'])

    # create the mappings in elasticsearch
    DicomDoc.init()

    dicom_doc = DicomDoc(meta={'id': rowkey}, title='Dicom')
    dicom_doc.data_exame = dicom_dataset.StudyDate
    dicom_doc.nome_paciente = dicom_dataset.PatientName
    dicom_doc.descricao_estudo = dicom_dataset.StudyDescription
    dicom_doc.descricao_serie = dicom_dataset.SeriesDescription
    dicom_doc.data_nasc_paciente = dicom_dataset.PatientBirthDate
    dicom_doc.especialidade_exame = dicom_dataset.Modality
    dicom_doc.save()


if __name__ == '__main__':

    root = __validate_dir(sys.argv[1])

    # Connect to Cassandra
    cluster = Cluster()


    session = cluster.connect('dicom')


    dicom_dict = dict({})

    patient_dict = {}
    study_dict = {}
    image_dict = {}

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

            image_bytes = sys.stdout.buffer.write(base64.b64encode(dicom_dataset.pixel_array))

            dicom_dataset_dict = __extract_dataset_to_dict(dicom_dict, dicom_dataset, image_bytes)

            rowkey, column_family = __define_column_family(dicom_dataset)

            __set_patient(patient_dict, dicom_dataset, image_bytes)

            __set_studies(study_dict, dicom_dataset, rowkey)

            __set_images(image_dict, dicom_dataset, image_bytes)

            __insert_into_elasticsearch(rowkey, dicom_dataset)

    __insert_into_cassandra(patient_dict, study_dict, image_dict)

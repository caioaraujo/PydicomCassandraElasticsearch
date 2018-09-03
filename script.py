import base64
import os
import sys

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

            image_bytes = sys.stdout.buffer.write(base64.b64encode(dicom_dataset.pixel_array))

            dicom_dataset_dict = __extract_dataset_to_dict(dicom_dict, dicom_dataset, image_bytes)

            rowkey, column_family = __define_column_family(dicom_dataset)

            __set_patient(patient_dict, dicom_dataset, image_bytes)

            __set_studies(study_dict, dicom_dataset, rowkey)

            __set_images(image_dict, dicom_dataset, image_bytes)

            __insert_into_elasticsearch(rowkey, dicom_dataset)

    __insert_into_cassandra(patient_dict, study_dict, image_dict)

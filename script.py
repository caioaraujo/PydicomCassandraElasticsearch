import base64
import os
import sys

import pydicom
from cassandra.cluster import Cluster


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


#def __insert_into_cassandra(cassandra_session):
#    session.execute(
#        """
#        INSERT INTO patient (name, credits, user_id)
#        VALUES (%s, %s, %s)
#        """,
#        ("John O'Reilly", 42, uuid.uuid1())
#    )


if __name__ == '__main__':

    root = __validate_dir(sys.argv[1])

    # Connect to Cassandra
    cluster = Cluster()

    session = cluster.connect('dicom')

    dicom_dict = dict({})

    for path, subdirs, files in os.walk(root):
        for name in files:
            file = os.path.join(path, name)

            file_extension = name.split('.')[-1]
            if file_extension.lower() != 'dcm':
                print('Ignoring file:', file)
                continue

            print('Working in file:', file)

            dicom_dataset = pydicom.dcmread(file)

            compressed_pixel_array = sys.stdout.buffer.write(base64.b64encode(dicom_dataset.pixel_array))

            dicom_dataset_dict = __extract_dataset_to_dict(dicom_dict, dicom_dataset, compressed_pixel_array)

            rowkey, column_family = __define_column_family(dicom_dataset)
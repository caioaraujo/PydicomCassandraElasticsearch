import os
import sys

import pydicom
from cassandra.cluster import Cluster


def __validate_dir(path):
    directory_does_not_exist_exception = Exception("Directory does not exists. Please type a valid path")

    if not os.path.isdir(path):
        raise directory_does_not_exist_exception

    return path


if __name__ == '__main__':

    root = __validate_dir(sys.argv[1])

    # Connect to Cassandra
    cluster = Cluster()

    session = cluster.connect('dicom')

    for path, subdirs, files in os.walk(root):
        for name in files:
            file = os.path.join(path, name)

            file_extension = name.split('.')[-1]
            if file_extension.lower() != 'dcm':
                print('Ignoring file:', file)
                continue

            print('Working in file:', file)

            dicom_dataset = pydicom.dcmread(file)
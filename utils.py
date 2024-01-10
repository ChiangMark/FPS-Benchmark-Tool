
import gzip
import shutil

def unzipfile(gz_file_path):
    print("unzip file {} ....".format(gz_file_path))
    # Specify the path for the extracted file
    extracted_file_path = gz_file_path.split('.gz')[0]

    # Open the GZ file and extract its contents
    with gzip.open(gz_file_path, 'rb') as gz_file:
        with open(extracted_file_path, 'wb') as extracted_file:
            shutil.copyfileobj(gz_file, extracted_file)
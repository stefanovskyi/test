import boto3
from boto3.s3.transfer import TransferConfig
import zipfile
import os
import time


def extract_zip(temp_folder, file_location):
    with zipfile.ZipFile(file_location, 'r') as zip_ref:
        zip_ref.extractall(temp_folder)


def upload_extracted_content(file_name, temp_folder):
    content_in_folder = os.listdir(temp_folder)

    for name in content_in_folder:
        if name.endswith('.zip'):
            print(name)
            file_full_location = temp_folder + name
            file_size = os.path.getsize(file_full_location) / 1000000
            os.remove(file_full_location)
            print('Removed ' + name + ' size in mb ' + str(file_size))
        else:
            print('Not a zip file: ' + name)
            file_full_name = temp_folder + name
            file_size = os.path.getsize(file_full_name) / 1000000

            upload_start_time = time.time()

            s3.upload_file(file_full_name, dataflik_bucket, 'attom/raw/' + name)
            upload_end_time = time.time()
            upload_duration_in_seconds = upload_end_time - upload_start_time
            print("Finished uploading %s in %s seconds" % (file_name, upload_duration_in_seconds))

            print("Uploaded: " + name + 'size in mb ' + str(file_size))
            os.remove(file_full_name)
            print("Removed: " + name + 'size in mb ' + str(file_size))


def download_file(s3, file_name, file_location, file_to_download, bucket_name):
    download_start_time = time.time()
    s3.download_file(bucket_name, file_to_download, file_location)
    download_end_time = time.time()
    download_duration_in_seconds = download_end_time - download_start_time
    print("Finished downloading %s in %s seconds" % (file_name, download_duration_in_seconds))


if __name__ == "__main__":
    start_time = time.time()

    temp_folder_for_unzipped_files = '/home/ec2-user/test/temp/'
    print("Temp folder: " + temp_folder_for_unzipped_files)

    dataflik_bucket = 'dataflik-data'
    s3_prefix = 'attom/zipped'

    s3 = boto3.client('s3')
    config = TransferConfig(multipart_threshold=1024 * 25,
                            max_concurrency=10,
                            multipart_chunksize=1024 * 25,
                            use_threads=True)

    all_objects = s3.list_objects_v2(
        Bucket=dataflik_bucket,
        Prefix=s3_prefix,
        MaxKeys=100)

    count = 0
    count_zip = 0
    total_files_in_bucket = str(len(all_objects['Contents']))
    print("There is: " + total_files_in_bucket + " objects")

    for obj in all_objects['Contents']:
        object_from_s3 = obj['Key']

        print(object_from_s3)

        if object_from_s3.endswith('.zip'):
            count_zip = count_zip + 1
            zip_filename = object_from_s3.split('/')[-1]

            local_file_location = temp_folder_for_unzipped_files + zip_filename
            s3_object_path = s3_prefix + '/' + zip_filename

            download_file(s3, zip_filename, local_file_location, s3_object_path, dataflik_bucket)
            extract_zip(temp_folder_for_unzipped_files, local_file_location)
            upload_extracted_content(zip_filename, temp_folder_for_unzipped_files)

        count = count + 1

        print(str(count) + " lines out of " + total_files_in_bucket)
        print(str(count_zip) + " zip files processed")

    end_time = time.time()
    duration_in_seconds = end_time - start_time
    print("Finished unzipping in %s seconds" % duration_in_seconds)

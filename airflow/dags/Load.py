import boto3
from dotenv import load_dotenv
import os
import json
import transformation as transformation

def load_youtube_data():
    # Transform the YouTube data and get the CSV folder path
    csv_folder_path = transformation.transform_youtube_data()
    bucket_name = 'yt-analysis-bucket'
    
    # Create the manifest file
    manifest_file_path = create_manifest_file(csv_folder_path, bucket_name)

    # Upload the manifest file and CSV files to S3
    upload_to_s3(csv_folder_path, bucket_name, manifest_file_path)

def upload_to_s3(folder_path, bucket_name, manifest_file_path):
    load_dotenv()
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = 'us-east-1'

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )

    # Upload the manifest file to the root of the S3 bucket
    try:
        s3_client.upload_file(manifest_file_path, bucket_name, 'manifest.json')
        print(f"Manifest file {manifest_file_path} successfully uploaded to S3 bucket.")
    except Exception as e:
        print(f"Error uploading manifest file to S3: {e}")

    # Upload all CSV files in the specified folder to the root of the S3 bucket
    csv_files = [file for file in os.listdir(folder_path) if file.endswith(".csv")]
    
    if not csv_files:
        print(f"No CSV files found in {folder_path}.")
        return  # Exit if no CSV files are found

    for file_name in csv_files:
        local_file_path = os.path.join(folder_path, file_name)
        try:
            s3_client.upload_file(local_file_path, bucket_name, file_name)  # Upload directly to the bucket
            print(f"File {local_file_path} successfully uploaded to S3 bucket as {file_name}.")
        except Exception as e:
            print(f"Error uploading file to S3: {e}")

def create_manifest_file(folder_path, bucket_name):
    csv_files = [file for file in os.listdir(folder_path) if file.endswith(".csv")]

    if not csv_files:
        print("No CSV files found in the specified folder.")
        return None  # Return None if no CSV files are found

    # Use the first CSV file for the manifest
    file = csv_files[0]

    # Prepare manifest data
    manifest_data = {
        "fileLocations": [
            {
                "URIs": [
                    f"s3://{bucket_name}/{file}"
                ]
            }
        ],
        "globalUploadSettings": {
            "format": "CSV",
            "delimiter": ",",
            "textqualifier": "\"",
            "containsHeader": "true"
        }
    }

    # Save the manifest file
    manifest_file_path = '/opt/airflow/data/manifest.json'  
    with open(manifest_file_path, 'w') as f:
        json.dump(manifest_data, f, indent=4)

    print(f"Manifest file created at {manifest_file_path}.")
    return manifest_file_path  # Return the path of the created manifest file

# Call the function to execute the process
load_youtube_data()

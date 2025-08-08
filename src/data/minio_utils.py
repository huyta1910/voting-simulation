import minio
from minio import Minio
import json
import os

def get_minio_client():
    return Minio(
        endpoint="localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

def upload_json_to_minio(client, bucket_name, object_name, data):
    # Ensure bucket exists
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
    # Save data to a temp file
    temp_file = f"{object_name}.json"
    with open(temp_file, "w") as f:
        json.dump(data, f)
    client.fput_object(bucket_name, object_name + ".json", temp_file)
    os.remove(temp_file)

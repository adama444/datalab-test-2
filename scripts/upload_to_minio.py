import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


def upload_to_s3():
    bucket_name = "togo-public-srv"
    file_path = "data/demandes_services_publics_togo.json"
    object_name = "raw/demandes_services_publics_togo.json"

    s3_client = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password123",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' verified.")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            print(f"Bucket '{bucket_name}' not found. Creating...")
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            raise e

    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"✅ Successfully uploaded to MinIO: {bucket_name}/{object_name}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")


if __name__ == "__main__":
    upload_to_s3()

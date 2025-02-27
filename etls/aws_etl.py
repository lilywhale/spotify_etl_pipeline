import boto3
import json
import pandas as pd
import logging
from io import StringIO
from botocore.exceptions import ClientError
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY


def get_secret(region_name: str, secret_name: str):

    # Create a Secrets Manager client
    session = boto3.session.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_ACCESS_KEY
    )
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:

        raise e

    secret_data = get_secret_value_response['SecretString']
    secret_dict = json.loads(secret_data)
    return secret_dict


def upload_to_s3(data: pd.DataFrame, bucket_name: str, file_name: str, region_name: str) -> None:
    """
    Uploads a DataFrame to S3 as a CSV file.
    """
    try:
        # Create a session and S3 client
        # Create an S3 client with explicit credentials
        session = boto3.session.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_ACCESS_KEY
        )
        client = session.client(service_name='s3', region_name=region_name)

        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)

        # Upload to S3
        client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
        logging.info(f"Successfully uploaded {file_name} to S3 bucket {bucket_name}.")
    except Exception as e:
        logging.error(f"Failed to upload {file_name} to S3: {e}")
        raise

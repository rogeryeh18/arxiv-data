import argparse
import os
import tempfile
import zipfile
from urllib.parse import urlparse

import boto3
import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


def download_kaggle_dataset(download_dir: str) -> str:
    logger.info('Downloading dataset from Kaggle API URL...')
    url = 'https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv'
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f'Failed to download: {response.status_code} {response.text}')

    os.makedirs(download_dir, exist_ok=True)
    zip_path = os.path.join(download_dir, 'arxiv.zip')
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(download_dir)

    for file in os.listdir(download_dir):
        if file.endswith('.json'):
            return os.path.join(download_dir, file)

    raise FileNotFoundError('arxiv-metadata-oai-snapshot.json not found after unzip.')


def upload_to_s3(local_path: str, bucket: str, s3_key: str):
    logger.info(f'Uploading {local_path} to s3://{bucket}/{s3_key}')
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket, s3_key)
    logger.info('Upload complete.')


def main():
    parser = argparse.ArgumentParser(
        description='Download arXiv snapshot from Kaggle and upload to S3 or local.'
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Output path: local folder or S3 URI (e.g., s3://bucket/path)',
    )

    args = parser.parse_args()
    output = args.output

    with tempfile.TemporaryDirectory() as temp_dir:
        local_json_path = download_kaggle_dataset(temp_dir)

        if output.startswith('s3://'):
            parsed = urlparse(output)
            bucket = parsed.netloc
            key_prefix = parsed.path.lstrip('/')
            s3_key = f'{key_prefix}/{os.path.basename(local_json_path)}'.lstrip('/')
            upload_to_s3(local_json_path, bucket, s3_key)
        else:
            os.makedirs(output, exist_ok=True)
            dest_path = os.path.join(output, os.path.basename(local_json_path))
            os.rename(local_json_path, dest_path)
            logger.info(f'Saved locally to {dest_path}')

    logger.info('Done.')


if __name__ == '__main__':
    main()

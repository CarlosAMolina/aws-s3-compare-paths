import csv
import logging
import os
import os.path
from collections import namedtuple
from pathlib import PurePath

import boto3

from config import export_config as config


S3Query = namedtuple("S3Query", "bucket prefix")
S3FileData = dict
S3Data = list[S3FileData]


logger = logging.getLogger(__name__)


def run():
    s3_queries = _get_s3_queries()
    for query_index, s3_query in enumerate(s3_queries, 1):
        print(f"Working with query {query_index}/{len(s3_queries)}: {s3_query}")
        s3_data = _get_s3_data(s3_query)
        _export_to_csv(s3_data, s3_query)


def _get_s3_queries() -> list[S3Query]:
    bucket = config["bucket"]
    path_names = config["paths"]
    return [
        S3Query(bucket, path_name)
        for path_name in path_names
    ]


def _get_s3_data(s3_query: S3Query) -> S3Data:
    session = boto3.Session()
    s3_client = session.client('s3')
    response = s3_client.list_objects_v2(Bucket=s3_query.bucket, Prefix=s3_query.prefix)
    if response["IsTruncated"] is True:
        folder_path = PurePath(s3_query.bucket, s3_query.prefix)
        raise ValueError(f"More files that the maximum allowed in {folder_path}. This script cannot manage all the files")
    for content in response['Contents']:
        # TODO bug: empty files are detected as folders.
        if content["Size"] == 0:
            folder_path = PurePath(s3_query.bucket, content["Key"])
            raise ValueError(f"Subfolder detected {folder_path}. This script cannot manage subfolders")
    return [
            {
                "name": _get_file_name_from_response_key(content),
                "date": content["LastModified"],
                "size": content["Size"],
            }
        for content in response['Contents']
    ]

def _get_file_name_from_response_key(content: dict) -> str:
    return content["Key"].split("/")[-1]


def _export_to_csv(s3_data: S3Data, s3_query: S3Query):
    bucket_path= _get_folder_path_for_bucket_export(s3_query.bucket)
    _create_folder_if_not_exists(bucket_path)
    file_path = _get_file_path(bucket_path, s3_query.prefix)
    _export_data_to_csv(s3_data, file_path)
    print(f"Extraction done")

def _create_folder_if_not_exists(path: PurePath):
    if not os.path.isdir(path):
        print("Creating folder", path)
        os.makedirs(path)


def _get_folder_path_for_bucket_export(bucket: str) -> PurePath:
    return PurePath("exports", bucket)


def _get_file_path(bucket_path: PurePath, s3_path_name: str) -> PurePath:
    s3_path_name_clean = s3_path_name[:-1] if s3_path_name.endswith('/') else s3_path_name
    file_name = s3_path_name_clean.replace('/','-')
    file_name = f"{file_name}.csv"
    return bucket_path.joinpath(file_name)


def _export_data_to_csv(s3_data: S3Data, file_path: PurePath):
    print(f"Exporting data to {file_path}")
    with open(file_path, "w", newline="") as f:
        w = csv.DictWriter(f, s3_data[0].keys())
        w.writeheader()
        for file_data in s3_data:
            w.writerow(file_data)


if __name__ == "__main__":
    run()

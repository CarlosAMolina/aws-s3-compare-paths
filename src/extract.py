import csv
import logging
import sys
from collections import namedtuple

import boto3


config = {
    "bucket": "pets",
    "paths": [
        "dogs/big/funds/",
        "cats/europe/",
    ]
}

S3Query = namedtuple("S3Query", "bucket prefix")
S3FileData = dict
S3Data = list[S3FileData]

logger = logging.getLogger(__name__)


def run():
    print(_get_s3_queries())
#    for s3_query in _get_s3_queries():
#        _run_s3_path_files_to_csv(s3_query)


def _get_s3_queries() -> list[S3Query]:
    bucket_name = config["bucket"]
    path_names = config["paths"]
    return [
        S3Query(bucket_name, path_name)
        for path_name in path_names
    ]

def _run_s3_path_files_to_csv(s3_query):
    print(f"Working with {s3_query}")
    s3_data = _get_s3_data(s3_query)
    _export_to_csv(s3_data, s3_query.prefix)


def _get_s3_data(s3_query: S3Query) -> S3Data:
    session = boto3.Session()
    s3_client = session.client('s3')
    response = s3_client.list_objects_v2(Bucket=s3_query.bucket, Prefix=s3_query.prefix)
    if response["IsTruncated"] is True:
        raise ValueError("I can't manage all the files")
    return [
            {
                "name": _get_file_name_from_response_key(content),
                "date": content["LastModified"],
                "size": content["Size"],
            }
        for content in response['Contents']
        if len(_get_file_name_from_response_key(content)) > 0 and content["Size"] > 0
    ]

def _get_file_name_from_response_key(content: dict) -> str:
    return content["Key"].split("/")[-1]


def _export_to_csv(s3_data: S3Data, s3_path_name: str):
    file_name = s3_path_name.replace('/','_')
    file_name = file_name[:-1] if file_name.endswith('_') else file_name
    file_name = f"{file_name}.csv"
    file_path_name = f"exports/{file_name}"
    with open(file_path_name, "w", newline="") as f:
        w = csv.DictWriter(f, s3_data[0].keys())
        w.writeheader()
        for file_data in s3_data:
            w.writerow(file_data)
    print(f"Extraction done: {file_path_name}")


if __name__ == "__main__":
    run()

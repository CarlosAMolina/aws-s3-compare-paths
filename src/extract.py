import csv
import logging
import sys
from collections import namedtuple

import boto3

S3Query = namedtuple("S3Query", "bucket prefix")
S3FileData = dict
S3Data = list[S3FileData]

logger = logging.getLogger(__name__)


def run():
    for s3_query in _get_s3_queries():
        _run_s3_path_files_to_csv(s3_query)


def _get_s3_queries() -> list[S3Query]:
    error_text = "The first argumen must ve i or f.\nUsage: python extract.py [i|f]"
    if len(sys.argv) > 1:
        if sys.argv[1] == "i":
            return [_get_s3_query_from_user_input()]
        if sys.argv[1] == "f":
            return _get_s3_queries_from_file()
    raise ValueError(error_text)


def _run_s3_path_files_to_csv(s3_query):
    print(f"Working with {s3_query}")
    s3_data = _get_s3_data(s3_query)
    _export_to_csv(s3_data, s3_query.prefix)


def _get_s3_query_from_user_input() -> S3Query:
    user_input = sys.argv
    try:
        bucket_name, path_name = user_input[2], user_input[3]
    except IndexError:
        raise ValueError(
            "Usage: python extract.py i {bucket} {prefix}"
            "\nExample: python extract.py i pets dogs/husky/"
        )
    return S3Query(bucket_name, path_name)

def _get_s3_queries_from_file() -> list[S3Query]:
    user_input = sys.argv
    file_path_name_with_paths_to_analyze = "files-with-paths/paths-to-analyze.txt"
    try:
        bucket_name = user_input[2]
    except IndexError:
        raise ValueError(
            "Usage: python extract.py f {bucket_name}"
            "\nExample: python extract.py f pets"
        )
    with open(file_path_name_with_paths_to_analyze, "r") as f:
        file_path_names = f.read().splitlines()
    return [S3Query(bucket_name, file_path_name) for file_path_name in file_path_names]


def _get_s3_data(s3_query: S3Query) -> S3Data:
    session = boto3.Session()
    s3_client = session.client('s3')
    response = s3_client.list_objects_v2(Bucket=s3_query.bucket, Prefix=s3_query.prefix)
    if response["IsTruncated"] is True:
        raise ValueError("I can't manage all the files")
    return [
            {
                "name": content["Key"].split("/")[-1],
                "date": content["LastModified"],
                "size": content["Size"],
            }
        for content in response['Contents']
    ]


def _export_to_csv(s3_data: S3Data, s3_path_name: str):
    file_name = f"{s3_path_name.replace('/','_')}.csv"
    file_path_name = f"exports/{file_name}"
    with open(file_path_name, "w", newline="") as f:
        w = csv.DictWriter(f, s3_data[0].keys())
        w.writeheader()
        for file_data in s3_data:
            w.writerow(file_data)
    print(f"Extraction done: {file_path_name}")


if __name__ == "__main__":
    run()

import csv
import logging
import sys
from collections import namedtuple

import boto3

S3Query = namedtuple("S3Query", "bucket prefix")
S3FileData = namedtuple("S3FileData", "name date size")
S3Data = list[S3FileData]

logger = logging.getLogger(__name__)


def run():
    s3_query = _get_s3_query_from_user_input()
    print(f"Working with {s3_query}")
    s3_data = _get_s3_data(s3_query)
    _export_to_csv(s3_data)


def _get_s3_query_from_user_input() -> S3Query:
    user_input = sys.argv
    try:
        bucket_name, path_name = user_input[1], user_input[2]
    except IndexError:
        raise ValueError(
            "Usage: python main.py {bucket} {prefix}"
            "\nExample: python main.py pets dogs/husky/"
        )
    return S3Query(bucket_name, path_name)


def _get_s3_data(s3_query: S3Query) -> S3FileData:
    session = boto3.Session()
    s3_client = session.client('s3')
    response = s3_client.list_objects_v2(Bucket=s3_query.bucket, Prefix=s3_query.prefix)
    if response["IsTruncated"] is True:
        raise ValueError("I can't manage all the files")
    return [
        S3FileData(
            obj["Key"].split("/")[-1],
            obj["LastModified"],
            obj["Size"],
        )
        for obj in response['Contents']
    ]


def _export_to_csv(s3_data: S3Data):
    with open('output.csv', 'w') as f:
        w = csv.writer(f)
        w.writerow(s3_data[0]._fields)    # field header
        w.writerows([(value for value in getattr(file_data, field_name)) for file_data in s3_data for field_name in file_data._fields])



if __name__ == "__main__":
    run()

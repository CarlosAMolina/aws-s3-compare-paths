import re

def _get_export_config() -> dict:
    _file_name_what_to_analyze = "s3-uris-to-analyze.txt"
    result = {}
    with open(_file_name_what_to_analyze, "r") as f:
        for s3_uri in f.read().splitlines():
            bucket_name, file_path_name = _get_bucket_and_path_from_s3_uri(s3_uri)
            if bucket_name not in result.keys():
                result[bucket_name] = []
            result[bucket_name].append(file_path_name)
        return result

def _get_bucket_and_path_from_s3_uri(s3_uri: str) -> tuple[str, str]:
    # https://stackoverflow.com/a/47130367
    match = re.match(r's3:\/\/(.+?)\/(.+)', s3_uri)
    bucket_name = match.group(1)
    file_path = match.group(2)
    return bucket_name, file_path
    # TODO regex_date = r"s3://(?P<year>\d{2})/(?P<month>\d{2}).xlsx"



export_config = _get_export_config()
AWS_ACCOUNT_WITH_DATA_TO_SYNC = "aws_account_1"

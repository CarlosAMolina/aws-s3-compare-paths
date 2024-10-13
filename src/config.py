def _get_bucket_to_analyze() -> str:
    return _get_content_file_what_to_analyze()[0]


def _get_paths_to_analyze() -> list[str]:
    return _get_content_file_what_to_analyze()[1:]


def _get_content_file_what_to_analyze() -> list[str]:
    _file_name_what_to_analyze = "what-to-analyze.txt"
    with open(_file_name_what_to_analyze, "r") as f:
        return f.read().splitlines()


export_config = {
    "bucket": _get_bucket_to_analyze(),
    "paths": _get_paths_to_analyze(),
}

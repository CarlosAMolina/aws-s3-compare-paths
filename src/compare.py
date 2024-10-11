import sys

import pandas as pd
from pandas import DataFrame as Df


FilePathNamesToCompare = tuple[str, str]


def run():
    file_path_names = _get_s3_file_name_paths_from_user_input()
    print(f"Start comparing {' and '.join(file_path_names)}")
    s3_data_df = _get_df_combine_files(file_path_names)
    _show_summary(file_path_names, s3_data_df)


def _get_s3_file_name_paths_from_user_input() -> FilePathNamesToCompare:
    user_input = sys.argv
    try:
        return user_input[1], user_input[2]
    except IndexError:
        raise ValueError(
            "Usage: python compare.py {file_path_name_1} {file_path_name_2}"
            "\nExample: python compare.py exports/file-1.csv exports/file-2.csv"
        )


def _get_df_combine_files(file_path_names: FilePathNamesToCompare) -> Df:
    file_1_df = _get_df_from_file(file_path_names, 0)
    file_2_df = _get_df_from_file(file_path_names, 1)
    return file_1_df.join(file_2_df, how='outer')


def _get_df_from_file(file_path_names: FilePathNamesToCompare, file_index: int) -> Df:
    return pd.read_csv(file_path_names[file_index], index_col="name").add_suffix(f'_file_{file_index}')


def _show_summary(file_path_names: FilePathNamesToCompare, df: pd.DataFrame):
    print(f"Files in {file_path_names[0]} but not in {file_path_names[1]}")
    print(_get_str_summary_lost_files(_get_lost_files(df, 0)))
    print(f"Files in {file_path_names[1]} but not in {file_path_names[0]}")
    print(_get_str_summary_lost_files(_get_lost_files(df, 1)))
    print("Files with different sizes")
    print(_get_str_summary_sizes_files(_get_files_with_different_size(df)))

def _get_str_summary_lost_files(files: list[str]) -> str:
    if len(files) == 0:
        return "- No lost files"
    return _get_str_from_files(files)

def _get_str_from_files(files: list[str]) -> str:
    files_with_prefix = [f"- {file}" for file in files]
    return "\n".join(files_with_prefix)

def _get_lost_files(df: Df, file_index: int) -> list[str]:
    return df.loc[df[f"date_file_{file_index}"].isnull()].index.tolist()

def _get_str_summary_sizes_files(files: list[str]) -> str:
    if len(files) == 0:
        return "- All files have same size"
    return _get_str_from_files(files)

def _get_files_with_different_size(df: Df) -> list[str]:
    condition = (df["size_file_0"].notnull()) & (df["size_file_1"].notnull()) & (df["size_file_0"] != df["size_file_1"])
    return df.loc[condition].index.tolist()

if __name__ == "__main__":
    run()

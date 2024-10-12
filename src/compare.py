import pandas as pd
from pandas import DataFrame as Df


FilePathNamesToCompare = tuple[str, str, str]

config = {
    "file_name": "file.csv", 
    "folder_names_with_files": ["work", "live", "pro"],
}


def run():
    _run_file_name()

def _run_file_name():
    s3_data_df = _get_df_combine_files()
    #_show_summary(s3_data_df, file_path_names)
    s3_analyzed_df = _get_df_analyze_s3_data(s3_data_df)
    print(s3_analyzed_df)
    s3_analyzed_df.to_csv('/tmp/foo.csv')


def _get_df_combine_files() -> Df:
    result = pd.DataFrame()
    for folder_name in config["folder_names_with_files"]:
        file_path_name = f"exports/{folder_name}/{config['file_name']}"
        file_df = _get_df_from_file(file_path_name, folder_name)
        result = result.join(file_df, how='outer')
    result.columns = pd.MultiIndex.from_tuples(_get_column_names_multindex(result))
    return result

def _get_column_names_multindex(column_names: list[str]) -> list[tuple[str, str]]:
    return [
        _get_tuple_column_names_multindex(column_name)
        for column_name in column_names
    ]

def _get_tuple_column_names_multindex(column_name: str) -> tuple[str, str]:
    indexes = column_name.split("_")
    return indexes[1], indexes[0]


def _get_df_from_file(file_path_name: str, environment: str) -> Df:
    return pd.read_csv(
        file_path_name,
        index_col="name",
        parse_dates=["date"],
    ).add_suffix(f"_{environment}")


def _get_df_analyze_s3_data(df: Df) -> Df:
    condition_exists = (
        df.loc[:, ("work", "size")].notnull()
    ) & (
        df.loc[:, ("live", "size")].notnull()
    ) & (
        df.loc[:, ("pro", "size")].notnull()
    )
    # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
    df[[("analysis","exists_file_in_all_paths"),]] = False
    df.loc[condition_exists, [("analysis","exists_file_in_all_paths"),]] = True
    # condition = (
    #     df.loc[:, ("analysis", "exists_file_in_all_paths")].eq(False)
    # ) & (
    #     df.loc[:, ("work", "size")].notnull()
    # )
    # df.loc[condition, "unique_path_where_the_file_exists"] = file_path_names[0]
    # condition = (
    #     df.loc[:, ("analysis", "exists_file_in_all_paths")].eq(False)
    # ) & (
    #     df.loc[:, ("live", "size")].notnull()
    # )
    # df.loc[condition, "unique_path_where_the_file_exists"] = file_path_names[1]
    condition = (
        df.loc[:, ("analysis", "exists_file_in_all_paths")].eq(True)
    ) & (
        df.loc[:, ("work", "size")] == df.loc[:, ("live", "size")]
    ) & (
        df.loc[:, ("live", "size")] == df.loc[:, ("pro", "size")]
    )
    df.loc[condition, ("analysis", "has_file_same_size_in_all_paths")] = False
    df.loc[condition, ("analysis", "has_file_same_size_in_all_paths")] = True
    return df


def _show_summary(df: pd.DataFrame, file_path_names: FilePathNamesToCompare):
    # TODO work with the result of _get_df_analyze_s3_data
    print()
    print(f"Files in {file_path_names[0]} but not in {file_path_names[1]}")
    print(_get_str_summary_lost_files(_get_lost_files(df, 0)))
    print()
    print(f"Files in {file_path_names[1]} but not in {file_path_names[0]}")
    print(_get_str_summary_lost_files(_get_lost_files(df, 1)))
    print()
    print("Files with different sizes")
    print(_get_str_summary_sizes_files(_get_files_with_different_size(df)))
    print()
    _show_last_file(file_path_names, df, 0)
    print()
    _show_last_file(file_path_names, df, 1)

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


def _show_last_file(file_path_names: FilePathNamesToCompare, df: Df, file_index: int):
    print("Last file in", file_path_names[file_index])
    column_name = f"date"
    condition = df[column_name] == df[column_name].max()
    row_file_df = df.loc[condition]
    file_name = row_file_df.index.values[0]
    date = row_file_df[column_name].values[0]
    print(f"{file_name} ({date})")


if __name__ == "__main__":
    run()

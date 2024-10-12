import pandas as pd
from pandas import DataFrame as Df


FilePathNamesToCompare = tuple[str, str, str]

config = {
    "file_name": "file.csv", 
    "folder_names_with_files": ["pro", "live", "work"],
}


def run():
    _run_file_name()

def _run_file_name():
    s3_data_df = _get_df_combine_files()
    #_show_summary(s3_data_df, file_path_names)
    s3_analyzed_df = _get_df_analyze_s3_data(s3_data_df)
    _export_to_csv(s3_analyzed_df)


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
    condition_pro_copied_wrong_in_live = (
          df.loc[:, ("pro", "size")].notnull()
    ) & ( df.loc[:, ("pro", "size")] != df.loc[:, ("live", "size")]
    )
    # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
    df[[("analysis","is_pro_copied_ok_in_live"),]] = None
    df.loc[condition_pro_copied_wrong_in_live, [("analysis","is_pro_copied_ok_in_live"),]] = False
    condition_pro_copied_wrong_in_work = (
          df.loc[:, ("pro", "size")].notnull()
    ) & ( df.loc[:, ("pro", "size")] != df.loc[:, ("work", "size")]
    )
    df[[("analysis","is_pro_copied_ok_in_work"),]] = None
    df.loc[condition_pro_copied_wrong_in_work, [("analysis","is_pro_copied_ok_in_work"),]] = False
    return df

def _export_to_csv(df: Df):
    to_csv_df = df.copy()
    to_csv_df.columns = ["_".join(pair) for pair in to_csv_df.columns]
    print(to_csv_df)
    to_csv_df.to_csv('/tmp/foo.csv')


if __name__ == "__main__":
    run()

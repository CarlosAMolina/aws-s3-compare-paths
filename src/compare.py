from pathlib import PurePath
import os

import pandas as pd
from pandas import DataFrame as Df


from constants import MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS
from constants import AWS_ACCOUNT_WITH_DATA_TO_SYNC


FilePathNamesToCompare = tuple[str, str, str]


def run():
    _run_file_name()

def _run_file_name():
    s3_data_df = _get_df_combine_files()
    #_show_summary(s3_data_df, file_path_names)
    s3_analyzed_df = _get_df_analyze_s3_data(s3_data_df)
    _export_to_csv(s3_analyzed_df)


def _get_df_combine_files() -> Df:
    result = Df()
    buckets_and_files: dict = _get_buckets_and_exported_files()
    for aws_account in _get_aws_accounts():
        account_df = _get_df_combine_files_for_aws_account(aws_account, buckets_and_files)
        result = result.join(account_df, how='outer')

    print(result)
    result.to_csv('/tmp/no_multi.csv')
    result.columns = pd.MultiIndex.from_tuples(_get_column_names_mult_index(result.columns))
    print(result)
    breakpoint()
    result.index = pd.MultiIndex.from_tuples(_get_index_multi_index(result.index))
    print(result)
    breakpoint()
    result.to_csv('/tmp/multi.csv')
    return result

def _get_df_combine_files_for_aws_account(aws_account: str, buckets_and_files: dict) -> Df:
    result = Df()
    for bucket_name, file_names in buckets_and_files.items():
        for file_name in file_names:
            file_path_name = PurePath(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS, aws_account, bucket_name, file_name)
            file_df = _get_df_from_file(file_path_name)
            file_df = file_df.add_prefix(f"{aws_account}_value_")
            file_df = file_df.set_index(f"{bucket_name}_file_" + file_df.index.astype(str))
            result = pd.concat([result, file_df])
    return result


def _get_buckets_and_exported_files() -> dict[str, list[str]]:
    bucket_names = os.listdir(PurePath(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS, AWS_ACCOUNT_WITH_DATA_TO_SYNC))
    bucket_names.sort()
    accounts = _get_aws_accounts()
    accounts.remove(AWS_ACCOUNT_WITH_DATA_TO_SYNC)
    accounts.sort()
    for account in accounts:
        buckets_in_account = os.listdir(PurePath(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS, account))
        buckets_in_account.sort()
        if bucket_names != buckets_in_account:
            raise ValueError("The S3 data has not been exported correctly. Error comparing buckets in account '{account}'")
    result = {}
    for bucket in bucket_names:
        file_names = os.listdir(PurePath(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS, AWS_ACCOUNT_WITH_DATA_TO_SYNC, bucket))
        file_names.sort()
        for account in accounts:
            files_for_bucket_in_account = os.listdir(PurePath(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS, account, bucket))
            files_for_bucket_in_account.sort()
            if file_names != files_for_bucket_in_account:
                raise ValueError(f"The S3 data has not been exported correctly. Error comparing files in account '{account}' and bucket '{bucket}'")
        result[bucket] = file_names
    return result


def _get_aws_accounts() -> list[str]:
    return os.listdir(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS)


def _get_df_from_file(file_path_name: PurePath) -> Df:
    result = pd.read_csv(
        file_path_name,
        index_col="name",
        parse_dates=["date"],
    )
    return result


def _get_column_names_mult_index(column_names: list[str]) -> list[tuple[str, str]]:
    return [
        _get_tuple_column_names_multi_index(column_name)
        for column_name in column_names
    ]


def _get_tuple_column_names_multi_index(column_name: str) -> tuple[str, str]:
    aws_account, file_value = column_name.split("_value_")
    return aws_account, file_value


def _get_index_multi_index(indexes: list[str]) -> list[tuple[str, str]]:
    return [_get_tuple_index_multi_index(index) for index in indexes]

def _get_tuple_index_multi_index(index: str) -> tuple[str, str]:
    bucket_name, file_name = index.split("_file_")
    return bucket_name, file_name

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

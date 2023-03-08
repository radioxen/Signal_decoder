import pandas as pd
import modin.pandas as mpd
from src.operators.signal_processor import *


def write_mediatory_dataset(file_name: str = "results.gzip", df_list: list = None):

    signals = pd.concat(df_list, ignore_index=True)
    signals.to_csv(
        path_or_buf=default_output_file_path + "/" + file_name,
        mode="a",
        compression="gzip",
        index=False,
        header=False,
    )


def write_mediatory_dataset_modin(
    file_name: str = "results.gzip", df_list: list = None
):

    signals = mpd.concat(df_list, ignore_index=True)
    signals.to_csv(
        path_or_buf=default_output_file_path + "/" + file_name,
        mode="a",
        compression="gzip",
        index=False,
        header=False,
    )


def write_mp_to_final_parquet(
    file_name: str = "mp_results.parquet", df: pd.DataFrame = None
):

    df.to_parquet(path=default_output_file_path + "/" + file_name, index=True)


def write_mpodin_to_final_parquet(
        input_path : str = default_output_file_path,
        file_name: str = "modin_results.parquet"
    ):
    df = mpd.read_csv( input_path + "/" +"modin_results.gzip").sort_values(by="Timestamp", ascending=True)
    df["Timestamp"] = df["Timestamp"].astype(float)
    df.drop_duplicates(subset=["Timestamp"], keep="last", inplace=True)
    df = df.explode("signal_value_pairs")
    df[["Signal", "Value"]] = pd.DataFrame(
        df.signal_value_pairs.tolist(), index=df.index
    ).sort_values(by="Timestamp")
    df["Bus_Signal"] = df["Bus"] + "_" + df["Signal"]
    df["Value"] = df["Value"].astype(float)
    df = df.pivot(index="Timestamp", columns="Bus_Signal", values="Value")
    df.to_parquet(path=default_output_file_path + "/" + file_name, index=True)


def build_intermediary_df(df):
    """
    The function receives a Modin/ pandas df,  filters the input dataframe based on a list of target signals,
    given by the target_signals variable. It then applies the get_signal_value_pairs function to each row of
    the filtered dataframe to decode hex values into signal-value pairs. Then 10ms timeframes are created, sorted
    in ascending order and then drop duplicated rows in each 10ms frame except the last one.
    Then the Signal_Value column is exploded into multiple rows for every signal-pair value and then
    the target columns ["Timestamp", "Bus", "Signal", "Value"] are appended to a csv.gzip file.

    The function then creates a new column called frames_10ms by multiplying the Timestamp column by 100, and converts it to an integer type. It sorts the dataframe by the Timestamp column and drops duplicate rows based on the frames_10ms column, keeping the last occurrence. It then updates the Timestamp column by dividing frames_10ms by 100 and converting it to an integer type.
    :param df: a Modin/ pandas dataframe including rows of variable length hex decoded values per signal.
    :return: a Modin/ pandas dataframe with filtered, decoded and fomatted signals.
    """

    df = df[df["ID (hex)"].isin(target_signals)]
    df["signal_value_pairs"] = df.apply(
        lambda x: get_signal_value_pairs(x.values.tolist()), axis=1
    )
    df = df[["Timestamp", "Bus", "signal_value_pairs"]]
    df["frames_10ms"] = df["Timestamp"] * 100
    df["frames_10ms"] = df["frames_10ms"].astype("int")
    df = df.sort_values(by="Timestamp", ascending=True)
    df.drop_duplicates(subset=["frames_10ms"], keep="last", inplace=True)
    df["Timestamp"] = df["frames_10ms"] // 100

    return df[["Timestamp", "Bus_Signal", "Value"]]


def filter_df(df):

    df["Value"] = df["Value"].astype("float")
    df["10ms_frame"] = (
        df["Timestamp"].astype("float").apply(lambda x: x * 100).astype("int")
    )
    df["Timestamp"] = df["Timestamp"].astype("float")
    df = (
        df.sort_values(by="Timestamp", ascending=True)
        .drop_duplicates(subset=["10ms_frame"], keep="last")
        .drop(columns="10ms_frame")
    )

    return df

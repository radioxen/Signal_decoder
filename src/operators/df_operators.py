import pandas as pd
import modin.pandas as mpd
from src.operators.signal_processor import *


def write_mediatory_dataset(file_name : str= "results.gzip",  df_list: list =None):

    signals = pd.concat(df_list, ignore_index=True)
    signals.to_csv(path_or_buf=default_output_file_path+"/"+file_name,
                   mode="a", compression="gzip", index=False,
                   header=False)


def write_mediatory_dataset_modin(file_name : str= "results.gzip",  df_list: list =None):

    signals = mpd.concat(df_list, ignore_index=True)
    signals.to_csv(path_or_buf=default_output_file_path+"/"+file_name,
                   mode="a", compression="gzip", index=False,
                   header=False)


def write_mp_to_final_parquet(file_name : str= "mp_results.parquet",  df:pd.DataFrame = None):

    df.to_parquet(path=default_output_file_path+"/"+file_name, index=False)


def build_intermediary_df(df):

    df = df[df["ID (hex)"].isin(target_signals)]
    df["signal_value_pairs"] = df.apply(
        lambda x: get_signal_value_pairs(x.values.tolist()), axis=1
    )

    # df = df.explode("signal_value_pairs")
    # df[['Signal','Value']] = pd.DataFrame(df.signal_value_pairs.tolist(), index= df.index)


    df = df.sort_values(by="Timestamp")
    df["frames_10ms"] = df["Timestamp"]*100
    df["frames_10ms"] = df["frames_10ms"].astype("int")
    df = df.sort_values(by="Timestamp")
    df.drop_duplicates(subset=["frames_10ms"], keep="last", inplace=True)
    df["Timestamp"] = df["frames_10ms"] // 100
    df["Timestamp"] = df["Timestamp"].astype("int")
    df = df.explode("signal_value_pairs")
    df[['Bus_Signal','Value']] = pd.DataFrame(df.signal_value_pairs.tolist(), index= df.index)
    df.drop(columns=["frames_10ms", "Bus", "signal_value_pairs"], inplace=True)

    return df[["Timestamp", "Bus_Signal","Value"]]


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

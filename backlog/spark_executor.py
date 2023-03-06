import pyspark.pandas as ps
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from src.operators.df_operators import *

ps.set_option("compute.max_rows", 10000)


if __name__ == "__main__":

    df = ps.read_csv(
        default_input_file_path,
        compression="gzip",
        header=0,
        sep=",",
        quotechar='"',
        on_bad_lines=None,
        low_memory=True,
        nrows=100000
    )

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
    df["Timestamp"] = df["frames_10ms"] / 100
    df = df.explode("signal_value_pairs")
    df[['Bus_Signal','Value']] = ps.DataFrame(df.signal_value_pairs.tolist(), index= df.index)
    df.drop(columns=["frames_10ms", "Bus", "signal_value_pairs"], inplace=True)
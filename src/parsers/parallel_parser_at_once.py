from constants import *
from multiprocessing import Process, Manager
from src.operators.df_operators import *
from src.operators.signal_processor import *


def mp_signal_processor_at_once(df, buffer):
    """
    Has the same functionality as of mp_signal_processor, except it skips Part_1 and does all the steps at once,
    awith only 1 buffer list. This method has lower memory (one less buffer list) and time complexity,
    since the signal_value pairs are (O(n-x)) left unpacked until the very last steps.
    :param df: dataframe containing raw hex signals
    :param buffer: multiprocessing Manager list object
    :return: None
    """

    df = df[df["ID (hex)"].isin(target_signals)]
    df["signal_value_pairs"] = df.apply(
        lambda x: get_signal_value_pairs(x.values.tolist()), axis=1
    )
    df["frames_10ms"] = df["Timestamp"] * 100
    df["frames_10ms"] = df["frames_10ms"].astype("int")
    df = df.sort_values(by="Timestamp")
    df.drop_duplicates(subset=["frames_10ms"], keep="last", inplace=True)
    df["Timestamp"] = df["frames_10ms"] / 100

    buffer.append(df[["Timestamp", "signal_value_pairs", "Value"]])


def mp_parser_at_once(input_path: str = None):
    chunks = pd.read_csv(
        input_path,
        compression="gzip",
        low_memory=True,
        chunksize=batch_size // 16,
        iterator=True,
    )
    proc_list = []
    manager = Manager()
    buffer = manager.list()
    for chunk in tqdm(chunks):
        p = Process(
            target=mp_signal_processor_at_once,
            args=(
                chunk,
                buffer,
            ),
        )
        p.start()
        proc_list.append(p)

    for proc in proc_list:
        proc.join()

    df = pd.concat(buffer)

    df = df.explode("signal_value_pairs")
    df[["Bus_Signal", "Value"]] = pd.DataFrame(
    df.signal_value_pairs.tolist(), index=df.index
    ).sort_values(by="Timestamp")
    df.drop_duplicates(subset=["Timestamp"], keep="last", inplace=True)
    final_df = df.pivot(index="Timestamp", columns="Bus_Signal", values="Value")
    write_mp_to_final_parquet(df=final_df)

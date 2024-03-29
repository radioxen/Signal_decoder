from constants import *
from multiprocessing import Pool, Process, Manager
from src.operators.df_operators import *
from src.operators.signal_processor import *


def mp_signal_processor(df : pd.DataFrame = None, buffer : list = None, pivot_buffer: list = None):
    """
    Part 1:
    Takes a pandas dataframe of binary data representing signals, then decodes each row and parse it
    only if the ID component of the signal corresponds to a desired list of IDs, The function then
    creates a Pandas DataFrame from the signals list, forming signal_value pairs for every hex signal.
    Then the Signal_Value column is exploded into multiple rows for every signal-pair value and then
    the target columns ["Timestamp", "Bus", "Signal", "Value"] are appended to a buffer list which will

    Part 2:
    be concatenated and saved in a csv file later.
    After part 1 is Done, The function performs the transformations and filter
    the 10-ms timeframes, drops duplicated rows for each frame and finally add a dataframe with
    ["Timestamp", "Bus_Signal", "Value"] columns to pivot_buffer list, which will be pivoted later.

    :param df: dataframe containing raw hex signals
    :param buffer: multiprocessing Manager list object
    :param pivot_buffer: multiprocessing Manager list object
    :return: None
    """

    # Complete part 1 and save to file,
    df = df[df["ID (hex)"].isin(target_signals)]
    df["signal_value_pairs"] = df.apply(
        lambda x: get_signal_value_pairs(x.values.tolist()), axis=1
    )
    df = df.explode("signal_value_pairs")
    df[["Signal", "Value"]] = pd.DataFrame(
        df.signal_value_pairs.tolist(), index=df.index
    )
    buffer.append(
        df[["Timestamp", "Bus", "signal_value_pairs"]]
    )  # Note : If we want to do part 1 and 2 together we can skip this line

    df = df[["Timestamp", "Bus", "Signal", "Value"]]
    # carry on with part 2 in the same thread
    df.drop(columns=["Signal","Value"], inplace=True)
    df["frames_10ms"] = df["Timestamp"] * 100
    df["frames_10ms"] = df["frames_10ms"].astype("int")
    df = df.sort_values(by="Timestamp")
    df.drop_duplicates(subset=["frames_10ms"], keep="last", inplace=True)
    df["Timestamp"] = df["frames_10ms"] / 100

    pivot_buffer.append(df[["Timestamp", "Bus", "signal_value_pairs"]])


def mp_parser(input_path: str = None):
    """
    perform mp_signal_processor in chunks using parallel Processes. The number of processes and chunk size can
    be adjusted within the code or by changing corresponding values in the .env file.
    :param input_path: str : input_file path
    :return: None
    """
    batch_count = 1
    chunks = pd.read_csv(
        input_path,
        compression="gzip",
        low_memory=True,
        chunksize=batch_size // (n_parallel_procs//2),
        iterator=True,
    )
    proc_num = n_parallel_procs #32 by default
    proc_list = []
    manager = Manager()
    buffer = manager.list()
    pivot_buffer = manager.list()
    for chunk in tqdm(chunks):
        p = Process(
            target=mp_signal_processor,
            args=(
                chunk,
                buffer,
                pivot_buffer,
            ),
        )
        p.start()
        proc_list.append(p)
        if batch_count % proc_num == 0:
            for proc in proc_list:
                proc.join()
            write_mediatory_dataset("mp_results.gzip", buffer)
            buffer = manager.list()
            proc_list = []
        batch_count += 1

    if buffer:
        for proc in proc_list:
            proc.join()
            write_mediatory_dataset("mp_results_final.gzip", buffer)

    df = pd.concat(pivot_buffer).sort_values(by="Timestamp")
    df.drop_duplicates(subset=["Timestamp"], keep="last", inplace=True)
    df = df.explode("signal_value_pairs")
    df[["Signal", "Value"]] = pd.DataFrame(
        df.signal_value_pairs.tolist(), index=df.index
    )
    df["Bus_Signal"] = df["Bus"] + df["Signal"]
    df = df.pivot(index="Timestamp", columns="Bus_Signal", values="Value")
    write_mp_to_final_parquet(df=df)

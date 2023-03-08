from constants import *
import ray
import modin.pandas as mpd
from src.parsers.parallell_parser import *


def modin_main(input_path: str = default_input_file_path):
    """
    Same functionality as of mp_pool_executor, done with Modin dataframe with ray backedn.
    :param input_path: str : input file path
    :return: None
    """
    ray.init(
        num_cpus=int(os.environ["NUM_CPUS"]),
        include_dashboard=True,
        local_mode=bool(int(os.environ["LOCAL_MODE"])),
        dashboard_host=os.environ["RAY_DASHBOARD_HOST"],
        dashboard_port=int(os.environ["RAY_DASHBOARD_PORT"]),
    )

    chunk_size = batch_size // (n_parallel_procs//2)
    df_chunks = mpd.read_csv(
        input_path,
        compression="gzip",
        iterator=True,
        chunksize=chunk_size,
        sep=",",
        verbose=False,
    )

    df_list = []
    counter = 0
    for chunk in tqdm(df_chunks):
        chunk = chunk[chunk["ID (hex)"].isin(columns_of_interest)]
        df_list.append(build_intermediary_df(chunk))
        counter += 1
        if counter % n_parallel_procs == 0:
            write_mediatory_dataset_modin(
                file_name="modin_results.gzip", df_list=df_list
            )
            counter = 0

    write_mediatory_dataset_modin(file_name="modin_results.gzip", df_list=df_list)

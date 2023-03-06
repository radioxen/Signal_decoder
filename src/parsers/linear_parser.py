from dotenv import load_dotenv

load_dotenv(".env")
import pandas as pd
from src.operators.signal_processor import *


def iterator(generator, batch_size: int = batch_size):

    counter = 0
    results = []
    pd.DataFrame([], columns=final_columns).to_csv(
        path_or_buf=default_output_file_path+"/single_thread.gzip", compression="gzip")
    for item in tqdm(generator):
        if counter == batch_size:
            csv_writer(output_path=default_output_file_path, df=results)
            results = []
        if item:
            results.append(item)
            counter += 1

    else:
        csv_writer(df=results)


def signal_decoder(data):
    for row in data:
        if row:
            yield signal_aggregator(row.decode().replace("\r\n", "").split(","))
        else:
            continue


def csv_writer(
    output_path: str = default_output_file_path, df: list = None,
):
    df = pd.DataFrame.from_records(df, columns=final_columns).explode(
        ["Signal", "Value"]
    )

    df.to_csv(path_or_buf=output_path+"/single_thread.gzip",
              mode="a", compression="gzip", index=False, header=False)

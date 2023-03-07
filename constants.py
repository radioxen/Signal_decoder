import os
from dotenv import load_dotenv
import warnings
warnings.simplefilter(action='ignore')

load_dotenv(".env")
import findspark

findspark.init()
import gzip
import mgzip
from functools import wraps
import tracemalloc
from time import perf_counter
import time
from tqdm import tqdm


final_columns = os.environ["FINAL_COLS"].split(",")
input_path = os.environ["INPUT_PATH"]
columns_of_interest = os.environ["COLS_OF_INTEREST"].split(",")
num_cpus = int(os.environ["NUM_CPUS"])
default_input_file_path = os.environ["INPUT_PATH"]
default_output_file_path = os.environ["OUTPUT_PATH"]
os.environ["__MODIN_AUTOIMPORT_PANDAS__"] = "1"
target_signals = ["7E3", "7E4", "1", "2", "3", "4", "5", "6"]
batch_size = int(os.environ["BATCH_SIZE_IN_Mb"]) * 1e6

mdf_data_types = {'Timestamp': float,
              'Bus': str,
              'Signal': str,
              'Value': float
              }



def measure_performance(func):
    """Measure performance of a function"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        tracemalloc.start()
        start_time = perf_counter()
        func(*args, **kwargs)
        current, peak = tracemalloc.get_traced_memory()
        finish_time = perf_counter()
        print(f"Function: {func.__name__}")
        print(f"Method: {func.__doc__}")
        print(
            f"Memory usage:\t\t {current / 10**6:.6f} MB \n"
            f"Peak memory usage:\t {peak / 10**6:.6f} MB "
        )
        print(f"Time elapsed is seconds: {finish_time - start_time:.6f}")
        print(f'{"-"*40}')
        tracemalloc.stop()

    return wrapper


def load_file(input_path: str = default_input_file_path):

    zip_file = gzip.open(input_path, "rb")

    return zip_file


def load_file_parallel(input_path: str = default_input_file_path):

    zip_file = mgzip.open(input_path, "rb")

    return zip_file

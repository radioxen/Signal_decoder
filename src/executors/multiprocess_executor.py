from src.parsers.parallell_parser import *
from src.parsers.parallel_parser_at_once import *



def mp_pool_executor(
    input_path: str = default_input_file_path,
):
    t_start = time.time()
    mp_parser(input_path)
    print(f"Parallel executor took : {time.time() - t_start} seconds to finish")


def mp_pool_executor_at_once(
        input_path: str = default_input_file_path,
):
    t_start = time.time()
    mp_parser_at_once(input_path)
    print(f"Parallel executor took : {time.time() - t_start} seconds to finish")

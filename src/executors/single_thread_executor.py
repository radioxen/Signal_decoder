from src.parsers.linear_parser import *

# @measure_performance
def single_executor(
    input_path: str = default_input_file_path, batch_size: int = batch_size
):
    t_start = time.time()
    data_reader = load_file(input_path)
    generator = signal_decoder(data_reader)
    iterator(generator, batch_size)
    data_reader.close()
    print(f"Single executor took : {time.time() - t_start} seconds to finish")

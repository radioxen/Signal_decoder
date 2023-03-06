import findspark
from constants import *

findspark.init()
from src.parsers.linear_parser import *


df = spd.read_csv(
    default_input_file_path,
    compression="gzip",
    header=0,
    sep=",",
    quotechar='"',
    on_bad_lines=None,
)


data = df.apply(lambda row: signal_aggregator(row.to_dict()), axis=1)

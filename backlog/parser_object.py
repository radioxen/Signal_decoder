from constants import *
from src.operators.signal_processor import *
import pandas as pd
import pyarrow as pa
from tqdm import tqdm
import pyarrow.parquet as pq

#TODO convert all executors into one object
class Data_parser:
    def __init__(
        self,
        input_file: str = default_input_file_path,
        data: list = None,
        df: pd.DataFrame = None,
        output_file_path: str = default_output_file_path,
    ):

        self.input_file = input_file
        self.data = data
        self.output_path = output_file_path
        self.df = df
        self.generator = None
        self.file_reader = None
        self.open_file = None

    def read_file(self):
        self.open_file = gzip.open(self.input_file, "rb")

    def signal_analysis(self):
        global final_columns
        for row in self.open_file:
            if row:
                row_list = row.decode().replace("\r\n", "").split(",")
                yield signal_aggregator(row_list)

    def process_in_batch_part_a(self, batch_size: int = 500000):

        counter = 0
        results = [[], [], [], []]
        self.generator = self.signal_analysis()
        for item in tqdm(self.generator):

            if counter == batch_size:
                pass
            # pqwriter = to_parquet(pqwriter, data)
            # break

            if item:
                results[0].extend(item[0])
                results[1].extend(item[1])
                results[2].extend(item[2])
                results[3].extend(item[3])
                counter += 1

    def list_to_df(self):
        self.df = pd.DataFrame(self.data, columns=final_columns)

    def filter_df(self):

        self.df["Value"] = self.df["Value"].astype("float")
        self.df["10ms_frame"] = (
            self.df["Timestamp"].astype("float").apply(lambda x: x * 100).astype("int")
        )
        self.df["Timestamp"] = self.df["Timestamp"].astype("float")
        self.df = (
            self.df.sort_values(by="Timestamp", ascending=True)
            .drop_duplicates(subset=["10ms_frame"], keep="last")
            .drop(columns="10ms_frame")
        )

    def filter_list(self):
        pass

    def stretcher(self):
        pass

    def build_arrow_tablefrom_df(self):

        fields = [
            (final_columns[0], pa.float64()),
            (final_columns[1], pa.string()),
            (final_columns[2], pa.string()),
            (final_columns[3], pa.float64()),
        ]
        schema = pa.schema(fields)

        schema = schema.remove_metadata()

        table = pa.Table.from_pandas(
            df=self.df, Schema_schema=schema, preserve_index=False
        ).replace_schema_metadata()

        return table

    def build_arrow_tablefrom_list(
        self, list_data: list = None, names: list = final_columns, dtypes: list = None
    ):

        table = pa.Table.from_pylist()
        return table

    def write_to_parquet(self, pqwriter, table):

        if not pqwriter:
            pqwriter = pq.ParquetWriter(self.output_path, table.schema)
        else:
            pqwriter.write_table(table)

        return pqwriter

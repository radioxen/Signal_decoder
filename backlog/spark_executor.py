from constants import *
import findspark
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

findspark.init()

load_dotenv("../.env")

spark_cluster_uri = os.environ["SPARK_URI"]
csv_path = os.environ["INPUT_PATH"]

spark = (
    SparkSession.Builder().master(spark_cluster_uri).appName("Beta_test").getOrCreate()
)

df = spark.read.option("header", True).csv(default_input_file_path)
df.dropna(how="all")
df.fillna(df["ID (hex)"].isin(target_signals))



#INCOMPLETE
# df["signal_value_pairs"] = df.apply(
#     lambda x: get_signal_value_pairs(x.values.tolist()), axis=1
# )
#
# df = df.explode("signal_value_pairs")
# df[['Signal','Value']] = pd.DataFrame(df.signal_value_pairs.tolist(), index= df.index)
# df = df[["Timestamp", "Bus", "Signal","Value"]]
#
# # carry on with part 2 in the same thread
# df["frames_10ms"] = df["Timestamp"]*100
# df["frames_10ms"] = df["frames_10ms"].astype("int")
# df = df.sort_values(by="Timestamp")
# df.drop_duplicates(subset=["frames_10ms"], keep="last", inplace=True)
# df["Timestamp"] = df["frames_10ms"] / 100
# df["Bus_Signal"] = df["Bus"] + df["Signal"]
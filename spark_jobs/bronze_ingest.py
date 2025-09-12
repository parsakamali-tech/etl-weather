from pyspark.sql import functions as F, types as T
from common import load_config, get_spark

# cfg_path is our conf path
def main(cfg_path="config/config.yaml"):
    # we get the confs for using
    cfg = load_config(cfg_path)
    spark = get_spark(cfg["spark"]["app_name"], cfg["spark"]["master"], cfg["spark"]["shuffle_partitions"])

    # our raw data path
    src = cfg["input"]["csv_path"]
    # our raw transforming into bronze path
    bronze_path = cfg["lake"]["bronze"]

    #  reading csv raw data
    df = (spark.read
          .option("header", True)
          .option("multiLine", True) # for fields witch has multi lines
          .option("escape", "\"") # the \n at the end of multi lines will be ignored
          .csv(src))

    # function of normalizing columns name with specific format
    def norm(c): return c.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
    # changing the names in action
    df = df.select([F.col(c).alias(norm(c)) for c in df.columns])

    # fixing column_name problem in our data : Loud Cover(loud_cover) --> cloud_cover
    if "loud_cover" in df.columns and "cloud_cover" not in df.columns:
        df = df.withColumnRenamed("loud_cover", "cloud_cover")

    # fixing date format if there is an problem
    df = df.withColumn(
        "event_ts",
        F.to_timestamp("formatted_date")
    )
    # geting y & m & d as columns from date
    df = (df
          .withColumn("year",  F.year("event_ts"))
          .withColumn("month", F.month("event_ts"))
          .withColumn("day",   F.dayofmonth("event_ts")))

    # writing dataset as parquet with partitions
    (df.write
       .mode("overwrite")
       .partitionBy("year", "month", "day")
       .parquet(bronze_path))

    spark.stop()

if __name__ == "__main__":
    main()

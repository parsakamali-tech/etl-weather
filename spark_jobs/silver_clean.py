from pyspark.sql import functions as F, types as T
from common import load_config, get_spark

def main(cfg_path="config/config.yaml"):
    cfg = load_config(cfg_path)
    spark = get_spark(cfg["spark"]["app_name"], cfg["spark"]["master"], cfg["spark"]["shuffle_partitions"])

    bronze = cfg["lake"]["bronze"]
    silver = cfg["lake"]["silver"]

    df = spark.read.parquet(bronze)

    # the desire type of columns(numbers columns transformed into double type)
    num_cols = {
        "temperature_c": T.DoubleType(),
        "apparent_temperature_c": T.DoubleType(),
        "humidity": T.DoubleType(),
        "wind_speed_km_h": T.DoubleType(),
        "wind_bearing_degrees": T.DoubleType(),
        "visibility_km": T.DoubleType(),
        "cloud_cover": T.DoubleType(),
        "pressure_millibars": T.DoubleType()
    }

    for c, t in num_cols.items():
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(t))

    # deleting " " & "\n" & "\t" & "\r" from strings
    for c in ["summary", "precip_type", "daily_summary"]:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))

    # checking if our requirments column(in config file at dq) are not droped
    req = cfg["dq"]["required_columns"]
    for c in req:
        if c in df.columns:
            pass
        else:
            df = df.withColumn(c, F.lit(None).cast(T.StringType())) # if it was deleted add and fill it with null --> string
    # add a column called dq_missing_required with True if the req column was droped     
    df = df.withColumn("dq_missing_required",
                       F.when(sum([F.col(c).isNull().cast("int") for c in req]) > 0, F.lit(True)).otherwise(F.lit(False)))

    # extracting date and hour as a independ column
    df = df.withColumn("event_date", F.to_date("event_ts")) \
           .withColumn("event_hour", F.hour("event_ts"))

    # droping the times that has no date
    df = df.filter(F.col("event_date").isNotNull())

    (df.write
       .mode("overwrite")
       .partitionBy("event_date")
       .parquet(silver))

    spark.stop()

if __name__ == "__main__":
    main()

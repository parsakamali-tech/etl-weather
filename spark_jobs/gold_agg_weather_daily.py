from pyspark.sql import functions as F
from common import load_config, get_spark

def main(cfg_path="config/config.yaml"):
    cfg = load_config(cfg_path)
    spark = get_spark(cfg["spark"]["app_name"], cfg["spark"]["master"], cfg["spark"]["shuffle_partitions"])

    silver = cfg["lake"]["silver"]
    gold   = cfg["lake"]["gold"]

    df = spark.read.parquet(silver)

    # analysis
    daily = (df.groupBy("event_date")
               .agg(
                   F.count("*").alias("records"),
                   F.avg("temperature_c").alias("avg_temp_c"),
                   F.min("temperature_c").alias("min_temp_c"),
                   F.max("temperature_c").alias("max_temp_c"),
                   F.avg("humidity").alias("avg_humidity"),
                   F.avg("pressure_millibars").alias("avg_pressure"),
                   F.avg("wind_speed_km_h").alias("avg_wind_speed_km_h"),
               )
             .orderBy("event_date"))

    # saving analysis in parquet format
    (daily.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(gold + "/daily"))

    # sending gold data in postgre with jdbc
    url = f"jdbc:postgresql://{cfg['postgres']['host']}:{cfg['postgres']['port']}/{cfg['postgres']['db']}"
    props = {
        "user": cfg["postgres"]["user"],
        "password": cfg['postgres']['password'],
        "driver": cfg["postgres"]["jdbc_driver"]
    }

    (daily.write
      .mode("append")
      .jdbc(url=url, table=cfg["postgres"]["table_daily"], properties=props))

    spark.stop()

if __name__ == "__main__":
    main()

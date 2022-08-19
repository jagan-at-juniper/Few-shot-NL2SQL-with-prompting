import pyspark
import pyspark.sql.functions as F
from pyspark.ml.stat import Correlation
from pyspark.sql.types import *
from datetime import datetime, timedelta
from pyspark.sql import Window
import time

utils = ["utilization_all", "utilization_tx", "utilization_rx_in_bss", "utilization_rx_other_bss", "utilization_unknown_wifi", "utilization_non_wifi"]
clients = ["med_clients", "max_clients", "avg_clients", "min_clients"]
test_cols = ["bcn_per_wlan", "noise_floor", "tx_phy_err", "mac_stats_tx_phy_err", "tx_errors", "rx_errors"]
mean_stds = ["uptime"]
main_cols_ids = ["site_id", "org_id", "ap_id", "band"]
main_cols = ["org_name", "site_name", "ap_name", "band"]
ids = ["site_id", "org_id", "ap_id"]
m_win = 60*2
m_slide = 60
detects = main_cols_ids + ["time"]
prevs = ["clients"] + ids
w1 = Window.partitionBy("org_id", "site_id", "ap_id", "band").orderBy("org_id", "site_id", "ap_id", "band", F.desc("time"))
w2 = F.window("time", windowDuration = f"{m_win} minutes", slideDuration = f"{m_slide} minutes")
checks = detects + ["bcn_per_wlan", "noise_floor", "tx_phy_err", "mac_stats_tx_phy_err"]


df_org = spark.read.parquet("s3://mist-secorapp-production/dimension/org").select(F.col("id").alias("org_id"),F.col("name").alias("org_name")).persist()

df_name = spark.read.parquet("s3://mist-secorapp-production/dimension/site/site.parquet").select(F.col("id").alias("site_id"),F.col("name").alias("site_name"),F.col("org_id").alias("org_id"))\
    .join(df_org,["org_id"]).select("org_name","site_name","org_id","site_id").persist()

df_device_name = spark.read.parquet("s3://mist-secorapp-production/dimension/device").select(F.col("mac").alias("ap_id"),F.col("name").alias("ap_name"),"site_id","model","type").persist()

# the roundTo unit is minutes
def roundTime(dt_in=None, roundTo=60):
    if not dt_in:
            return None
    if isinstance(dt_in,str):
        try:
            dt = datetime.strptime(dt_in[0:19].replace("T"," "), '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(e)
            return None
    elif not isinstance(dt_in,datetime):
        return None
    else:
        dt = dt_in
    seconds = (dt- dt.min).seconds
    roundTo = roundTo*60
    rounding = (seconds+roundTo/2) // roundTo * roundTo
    return dt + timedelta(0,rounding-seconds,-dt.microsecond)


def bcn_per_wlan(bcn, num_wlans, model=""):
    bcn = bcn if bcn != None else -1.0
    bcn_high = 700.0
    if model.find("AP41") > -1:
        bcn_norm = bcn if bcn < bcn_high else bcn_high
    else:
        bcn_norm = bcn / num_wlans if num_wlans > 0 else 0
    bcn_norm = bcn_norm if bcn_norm < bcn_high else bcn_high
    return float(bcn_norm)

bcn_per_wlan_norm = F.udf(bcn_per_wlan, FloatType())
    
def read_data(dt, hr):
    df = spark.read.parquet("s3://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/dt={dt}/hr={hr}".format(dt=dt,hr=hr))\
    .withColumn("radios", F.explode("radios"))\
            .select(*ids, "model", "radios.*", F.to_timestamp(F.col("when")/1000000).alias("timestamp"), "uptime")\
            .withColumn("num_wlans", F.size("wlans"))\
            .withColumn("bcn_per_wlan", bcn_per_wlan_norm(F.col("interrupt_stats_tx_bcn_succ"), F.col("num_wlans"), F.col("model")))\
            .where(F.col("bandwidth") != 0)\
            .filter((F.col("num_wlans") > 0) & (F.col("radios.radio_missing")==False) & (F.col("uptime") > 24*60*60))

    return df

def filtering_data(df):
    first_cols = ["channel", "bandwidth", "re_init", "re_init_throttle", "tx_failed", "tx_retried", "tx_pkts", "rx_dups", "rx_pkts"] + test_cols + utils + ["wlans"]
    roundTimeUDF = F.udf(roundTime,TimestampType())
    df_filter = df.withColumn("time",roundTimeUDF("timestamp"))\
                .withColumn("time", F.from_utc_timestamp('time', 'PST'))\
                .groupby(*ids, "band", "time", "model")\
                .agg(*[F.max(col).alias(col) for col in first_cols],
                *[F.mean(col).alias("avg_" + col) for col in mean_stds], 
                *[F.stddev(col).alias("std_" + col) for col in mean_stds], 
                F.mean("num_clients").alias("avg_clients"), F.max("num_clients").alias("max_clients"), F.min("num_clients").alias("min_clients"),
                F.percentile_approx("num_clients", 0.5).alias("med_clients"),
                F.stddev("num_clients").alias("std_clients")).withColumn("ap_id", F.regexp_replace("ap_id", "-", ""))

    return df_filter
    
    
# join everything in here
def change_id_to_names(df, df_name=df_name, df_device_name=df_device_name, df_org=df_org):
    df_get_names = df.join(df_device_name, ["ap_id", "site_id"]).join(df_name, ["org_id", "site_id"]).distinct()
    return df_get_names

    
    
env ="production"
s3_bucket = "s3://mist-secorapp-{env}/cv-ap-scans-multipartition/cv-ap-scans-multipartition-{env}/dt={dt}/hr={hr}".format(env=env, dt=dt, hr="*")

def get_neighbors(df_get_names, ap_s, band, s3_bucket=s3_bucket):
    df = spark.read.format("orc").load(s3_bucket)
    neighbors = df.filter(F.col("in_site")==True).groupby("org","site","ap","ap2","band").agg(F.mean("rssi").alias("avg_rssi"))\
            .sort(F.desc("avg_rssi")).filter((F.col("ap") == ap_s) & (F.col("band") == band))\
            .limit(5).select("ap2", "band")
    
    collections = neighbors.collect()
    print(collections)
    df_aps = None
    df_timeframes = None
    cond_1 = (F.col("ap_id") == ap_s) & (F.col("band") == band)

    for ap in collections:
        ap_id = ap[0]
        band_rad = ap[1]
        cond_1 |= (F.col("ap_id") == ap_id) & (F.col("band") == band_rad)

    df_aps = df_get_names.filter(cond_1).sort("time")
    del df
    return df_aps


if __name__ == "__main__":
    start = time.time()
    dt= "2022-08-05"
    df_read = read_data(dt, "*")
    df_get_names = filtering_data(df_read)
    
    df_get_names = df_get_names.withColumn("clients", F.col("max_clients"))
    
    percent = 75
    df_thresolds = df_get_names.filter(F.col("clients") > 0).groupby("site_id", "org_id")\
        .agg(F.expr(f'percentile(clients, array(0.{percent}))')[0].alias("thre_clients")).select("site_id", "org_id", "thre_clients")
    
    
    df_long_0 = df_get_names.select(*checks, *[F.lag(c, offset=-1).over(w1).alias("prev_" + c) for c in prevs], 
                                *[F.lag(c, offset=-2).over(w1).alias("prev_2_" + c) for c in prevs])\
                .join(df_thresolds, ["site_id", "org_id", "ap_id"])
    
    df_edges = df_long_0.sort("time").groupby(*main_cols_ids, w2)\
            .agg(*[F.max(col).alias(col) for col in ["bcn_per_wlan", "noise_floor", "tx_phy_err"]],
                 F.avg("clients").alias("avg_clients"),
                 F.max("clients").alias("max_clients"),
                 F.count("clients").alias("count_clients"),
                 F.first("time").alias("time"),
                 F.max("thre_clients").alias("thre_clients"),
                 F.first("prev_2_clients").alias("prev_2_clients"),
                 F.first("prev_clients").alias("prev_clients"))\
            .filter(F.col("count_clients") >= m_win/m_slide)\
            .filter((F.col("max_clients") == 0) & (F.col("avg_clients") == 0))\
            .filter((F.col("prev_2_clients") >= F.col("thre_clients")) & (F.col("prev_clients") >= F.col("thre_clients")))
    
    df_edges = df_edges.withColumn("hr", F.hour("time"))
    spark.conf.set("spark.sql.broadcastTimeout",  36000)
    
    df_edges.write.partitionBy("hr")\
            .mode("overwrite").parquet("s3://mist-data-science-dev/tung/all/edges/dt={dt}/hr=*/".format(dt=dt))
    
    df_result = change_id_to_names(df_edges)

    df_result.show(truncate = False)
    
    end = time.time()
    print("The time is:", start-end)
import json
from pyspark.sql import functions as F
import json

def checkz_rrm():

    env = "production"

    s3_bucket = "s3://mist-secorapp-{env}/rrm-local/rrm-local-{env}/".format(env=env)

    date_day = "2022-04-*"
    hr = '*'

    rrm_local_path = "dt={day}/hr={hr}/*.seq".format(env=env, day=date_day, hr=hr)
    rrm_local_path = s3_bucket + rrm_local_path
    print(rrm_local_path)

    rdd_rrm_local = spark.sparkContext.sequenceFile(rrm_local_path)
    df_rrm = rdd_rrm_local.map(lambda x: json.loads(x[1])).toDF()
    df_rrm.printSchema()

    # s3_bucket = "s3://mist-secorapp-production/rrm-local/rrm-local-production/dt=2022-01-*/hr=*/*.seq"
    # rrm_local_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    # rrm_local_df = rrm_local_rdd.toDF()
    # rrm_local_df.printSchema()
    # # rrm_local_df.count()

    site_id = 'cf82bb8c-ab7d-4cc2-9cc6-921ceee44d56'  # BLG985
    df_rrm_site = df_rrm.filter(F.col("site_id")==site_id) #.filter(F.col("reason") == "rrm-global")
    df_rrm_site.select("band", "pre_usage", "usage").groupBy("band", "pre_usage", "usage").count().show()

    df_rrm_site.filter("pre_usage='24' and usage='5'").show()


df_test = df_rrm.select("command", "reason", F.length("ap_id").alias("ap_id_length")).groupBy("command", "reason", "ap_id_length").count()
df_test.show()

df_test = df_rrm.filter(F.length("ap_id").alias("ap_id_length")>12).groupBy("command", "reason").count()
df_test.show()


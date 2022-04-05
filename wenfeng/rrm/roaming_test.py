
import json

# client-roam
# client-pingpong-roaming/
# client-suboptimal-roaming/
# sticky-client/



def check_roam():
    import json
    s3_bucket =  "s3://mist-secorapp-production/client-roam/client-roam-production/" \
            "dt=2021-03-129/hr=20/*.seq"  #.format("production", "")

    print(s3_bucket)
    client_roam_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    client_roam_df = client_roam_rdd.toDF()
    client_roam_df.printSchema()
    client_roam_df.count()


def client_suboptimal_roaming():
    s3_bucket = "s3://mist-secorapp-production/client-suboptimal-roaming/client-suboptimal-roaming-production/" \
                "dt=2021-02-11/hr=*/*.seq"  #.format("production", "")
    print(s3_bucket)
    client_suboptimal_roam_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    client_suboptimal_roam_df = client_suboptimal_roam_rdd.toDF()
    client_suboptimal_roam_df.printSchema()
    client_suboptimal_roam_df.count()


def client_pingpong():
    s3_bucket = "s3://mist-secorapp-production/client-pingpong-roaming/client-pingpong-roaming-production/" \
                "dt=2021-02-11/hr=*/*.seq"  #.format("production", "")
    print(s3_bucket)
    client_pingpong_roam_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    client_pingpong_roam_df = client_pingpong_roam_rdd.toDF()
    client_pingpong_roam_df.printSchema()
    client_pingpong_roam_df.count()


def sticky_client():
    import json
    s3_bucket = "s3://mist-secorapp-production/sticky-client/sticky-client-production/" \
                "dt=2021-03-29/hr=20/*.seq"  #.format("production", "")
    print(s3_bucket)
    sticky_client_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    sticky_client_df = sticky_client_rdd.toDF()
    sticky_client_df.printSchema()
    # sticky_client_df.count()

    sticky_1 = sticky_client_rdd.filter(lambda x: x.get("Assoc").get("AP")=="5c-5b-35-53-ac-a5")
    sticky_2 = sticky_client_rdd.filter(lambda x: x.get("Assoc").get("AP")=="5c-5b-35-d0-3e-c6")

    from operator import add
    sticky_1.map(lambda x: (x.get("WC"), 1)).reduceByKey(add).collect()
    sticky_2.map(lambda x: (x.get("WC"), 1)).reduceByKey(add).collect()

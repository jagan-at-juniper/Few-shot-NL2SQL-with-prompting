
import json

# client-roam
# client-pingpong-roaming/
# client-suboptimal-roaming/
# sticky-client/



def check_roam():
    import json
    s3_bucket =  "s3://mist-secorapp-production/client-roam/client-roam-production/" \
            "dt=2021-02-16/hr=*/*.seq"  #.format("production", "")

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
    s3_bucket = "s3://mist-secorapp-production/sticky-client-production/sticky-client-production/-production/" \
                "dt=2021-02-11/hr=*/*.seq"  #.format("production", "")
    print(s3_bucket)
    sticky_client_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    sticky_client_df = client_suboptimal_roam_rdd.toDF()
    sticky_client_df.printSchema()
    sticky_client_df.count()
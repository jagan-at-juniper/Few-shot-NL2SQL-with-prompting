def checkz_rrm():
    import json
    s3_bucket = "s3://mist-secorapp-production/rrm-local/rrm-local-production/dt=2021-02-16/hr=*/*.seq"
    rrm_local_rdd = spark.sparkContext.sequenceFile(s3_bucket).map(lambda x: json.loads(x[1]))
    rrm_local_df = rrm_local_rdd.toDF()
    rrm_local_df.printSchema()
    rrm_local_df.count()
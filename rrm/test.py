from pyspark.shell import spark

env = "production"
# env = "staging"


# s3_bucket = "s3://mist-aggregated-stats-{env}/entity_event/entity_event-{env}/".format(env=env)
date_day = "2020-03-1*"
hr = '*'

# s3_capacity_path = "dt={day}/hr={hr}/CapacityAnomalyEvent_*.seq".format(day=date_day, hr=hr)
# s3_coverage_path = "dt={day}/hr={hr}/CoverageAnomalyEvent_*.seq".format(day=date_day, hr=hr)
# s3_capacity_path = s3_bucket + s3_capacity_path
# s3_coverage_path = s3_bucket + s3_coverage_path
# print(s3_capacity_path, "\n", s3_coverage_path)

s3_bucket = "s3://mist-secorapp-staging/sle-capacity-anomaly/sle-capacity-anomaly-staging/".format(env=env)
s3_path = s3_bucket + "dt={day}/hr={hr}/*.seq".format(day=date_day, hr=hr)

print(s3_path)
rdd_capacity = spark.sparkContext.sequenceFile(s3_path)

rdd_capacity.first()


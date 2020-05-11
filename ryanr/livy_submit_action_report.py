from emr import EMRSparkStep, Livy


# def __init__(self, env, main, py_files, args=None,
#              files=None, extra_spark_args=dict(), **kwargs)
# EMRSparkStep("data-science-staging", )

# data to be sent to api
data = { "className": "org.apache.spark.examples.SparkPi",
         "pyFiles":["s3://mist-data-science-dev/ryanr/action_report.py"],
         "args": ["2020-05-02"]}

data_science_dev_4 = "http://ec2-3-93-213-4.compute-1.amazonaws.com"

livy = Livy(data_science_dev_4)
livy.submit_statement(data)

# (self, host: str, kind: str = 'pyspark', port: int = 8998)

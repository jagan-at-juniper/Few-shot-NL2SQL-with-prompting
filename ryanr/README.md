



data = 
{
“className”: “org.apache.spark.examples.SparkPi”,
“jars”: [“a.jar”, “b.jar”],
“pyFiles”: [“a.py”, “b.py”],
“files”: [“foo.txt”, “bar.txt”],
“archives”: [“foo.zip”, “bar.tar”],
“driverMemory”: “10G”,
“driverCores”: 1,
“executorCores”: 3,
“executorMemory”: “20G”,
“numExecutors”: 50,
“queue”: “default”,
“name”: “test”,
“proxyUser”: “foo”,
“conf”: {“spark.jars.packages”: “xxx”},
“file”: “hdfs:///path/to/examples.jar”,
“args”: [1000]
}
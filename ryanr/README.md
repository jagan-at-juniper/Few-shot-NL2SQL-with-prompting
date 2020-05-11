

#### Example SSH command to connect to EMR cluster
ssh -i $HOME/.ssh/id_rsa hadoop@ec2-3-93-213-4.compute-1.amazonaws.com
ssh -i $HOME/.ssh/id_rsa hadoop@ec2-54-147-14-244.compute-1.amazonaws.com
ssh -i $HOME/.ssh/id_rsa hadoop@ec2-3-88-32-91.compute-1.amazonaws.com
s3://mist-staging-assets/services/spark_jobs/emr-bootstrap-latest.sh
s3://mist-data-science-dev/emr-bootstrap-dev/emr-bootstrap-ssh-env.shfilter

### Run code with spark submit using the following command
spark-submit --deploy-mode cluster --master yarn ./action_report.py 2020-05-09
### Run code with spark submit using the following command on S3
spark-submit --deploy-mode cluster --master yarn s3://mist-data-science-dev/ryanr/action_report.py 2020-05-08

aws s3 ls s3://bucket-name
aws s3 cp file.txt s3://my-bucket/
aws s3 ls s3://mist-data-science-dev/automated_action_report

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
#### Example SSH command to connect to EMR cluster
ssh -i $HOME/.ssh/id_rsa hadoop@ec2-3-93-213-4.compute-1.amazonaws.com
ssh -i $HOME/.ssh/id_rsa hadoop@ec2-54-147-14-244.compute-1.amazonaws.com
ssh -i $HOME/.ssh/id_rsa hadoop@ec2-3-88-32-91.compute-1.amazonaws.com
s3://mist-staging-assets/services/spark_jobs/emr-bootstrap-latest.sh
s3://mist-data-science-dev/emr-bootstrap-dev/emr-bootstrap-ssh-env.shfilter

#### Run code with spark submit using the following command
spark-submit --deploy-mode cluster --master yarn ./action_report.py 2020-05-09
#### Run code with spark submit using the following command on S3
spark-submit --deploy-mode cluster --master yarn s3://mist-data-science-dev/ryanr/action_report.py 2020-05-08

#### AWS push code with cp
aws s3 cp action_report.py s3://mist-data-science-dev/ryanr/
#### AWS retrieve list of files that have run
aws s3 ls s3://mist-data-science-dev/automated_action_report/

#### Talking to the livy server
curl http://ec2-3-93-213-4.compute-1.amazonaws.com:8998/sessions
#### On the cluster
localhost:8998/sessions

#### Requests
curl -X POST -data '{"kind": "pyspark"}' -H "Content-Type: application/json" localhost:8998/sessions

#### Submitting HTTP Request for Livy Batch Session
aws-okta exec mist-data-engineer -- curl -H "Content-Type: application/json" -X POST http://ec2-54-147-14-244.compute-1.amazonaws.com:8998/batches  -d '{"className": "org.apache.spark.examples.SparkPi", "args": ["2020-05-02"], "file": "s3://mist-data-science-dev/ryanr/action_report.py"}' 

#### or the following without okta
curl -H "Content-Type: application/json" -X POST http://ec2-54-147-14-244.compute-1.amazonaws.com:8998/batches -d '{"className": "org.apache.spark.examples.SparkPi", "args": ["2020-05-02"], "file": "s3://mist-data-science-dev/ryanr/action_report.py"}'

#### Can retrieve info about the session using (number after batch is the session id)
curl -X GET http://ec2-54-147-14-244.compute-1.amazonaws.com:8998/batches/4/state
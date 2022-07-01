 ./dev-scripts/generate_pyzip ../spark_jobs_test.zip ;
 aws-okta exec mist-data-science --  aws s3 cp ../spark_jobs_test.zip s3://mist-data-science-dev/wenfeng/spark_jobs_test.zip;
 gsutil cp ../spark_jobs_test.zip gs://mist-data-science-dev/wenfeng/spark_jobs_test.zip

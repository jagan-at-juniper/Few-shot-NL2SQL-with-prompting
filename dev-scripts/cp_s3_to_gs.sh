
cp_file=
aws-okta exec mist-data-science --  aws s3 ls s3://mist-production-jupyterhub-notebooks/jupyter/wenfeng/EMR-notebooks/test_es_suggestions-check.ipynb .

gsutil cp test_es_suggestions-check.ipynb  gs://mist-data-science-dev/wenfeng/spark_jobs_test.zip


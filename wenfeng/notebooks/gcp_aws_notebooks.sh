
 gsutil cp gs://mist-data-science-dev/jupyter/Wenfeng/ap-stats-full-analytics-no-clients.ipynb a_tmp.ipynb
 aws-okta exec mist-data-science --  aws s3 cp ~/a.ipynb s3://mist-production-jupyterhub-notebooks/jupyter/wenfeng/EMR-notebooks/ap-stats-full-analytics-no-clients.ipyn




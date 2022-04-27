
FileName=$1
currDir=$(pwd)
echo "copy file aws to local: "$FileName
aws-okta exec mist-data-science --  aws s3 cp s3://mist-production-jupyterhub-notebooks/jupyter/wenfeng/EMR-notebooks/$FileName $currDir

echo "copy local file  to gcp : "$FileName

gsutil cp $currDir/$FileName  gs://mist-production-jupyterhub-notebooks/jupyter/wenfeng/EMR-notebooks/


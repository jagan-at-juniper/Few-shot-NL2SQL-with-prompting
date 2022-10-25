
sourceFileName=$1
targetFileName=$2
echo "cp " $sourceFileName $targetFileName
echo "aws" 
echo $targetFileName
aws-okta exec mist-data-science --  aws s3 cp $sourceFileName s3://mist-data-science-dev/$targetFileName
echo "gcp"
gsutil cp $sourceFileName gs://mist-data-science-dev/$targetFileName
echo "done"

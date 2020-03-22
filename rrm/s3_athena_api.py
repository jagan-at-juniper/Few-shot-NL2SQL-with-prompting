import os

import boto3
import datetime
import time
from botocore.exceptions import ClientError


def run_query(query, database, s3_output, region_name='us-east-1'):
    """

    :param query:  athena query
    :param database: athena database name
    :param s3_output: s3 bucket storing output-temporary-file
    :param region_name: aws reagion name
    :return: response block
    """
    client = boto3.client('athena', region_name=region_name)
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output
        }
    )
    # print('Execution ID: ' + response['QueryExecutionId'])
    print(response)
    return response


def download_s3_file(response, s3_bucket, region_name='us-east-1', download_local_path="./"):
    """

    :param response: athena-response
    :param s3_bucket: s3 bucket where stores files to be downloaded
    :param region_name: aws region name
    :param download_local_path: local path
    :return: download filename
    """
    if response.get('ResponseMetadata').get('HTTPStatusCode') != 200:
        print("response error!")
        return "ERROR"

    query_id = response.get('QueryExecutionId')
    s3_output_file = 'results/' + query_id + '.csv'
    download_local_filename = download_local_path + query_id + '.csv'
    print(s3_output_file, download_local_filename)
    while True:
        client = boto3.client('s3', region_name=region_name)
        # s3_output_response = ""
        try:
            s3_output_response = client.get_object(Bucket=s3_bucket, Key=s3_output_file)
            print(s3_output_response)
            client.download_file(Bucket=s3_bucket, Key=s3_output_file, Filename=download_local_filename)
            break
        except ClientError:
            print('checking if key exist')
        time.sleep(10)

    return download_local_filename


def get_ap_scan(env, site, band="5"):
    """

    :param env: env=production/staging...
    :param site: site ID
    :param band: band=5/24
    :return: local filename
    """
    today = datetime.datetime.today().strftime("%Y-%m-%d")

    s3_bucket = 'mist-{ENV}-athena'.format(ENV=env)
    athena_database = 'secorapp_{ENV}'.format(ENV=env)
    table = 'cv_ap_scans'
    region_name = 'us-east-1'

    s3_output_bucket = 's3://%s/results/' % s3_bucket
    print("s3_output=", s3_output_bucket)

    download_local_path = os.curdir + './../downloads/'
    if not os.path.isdir(download_local_path):
        os.mkdir(download_local_path)
    print("download_local_path = {}".format(download_local_path))

    # band='5'
    query = '''SELECT * FROM "secorapp_{ENV}"."{TABLE}" where site='{SITE}' and band={BAND} and dt='{today}';
    '''.format(ENV=env, TABLE=table, SITE=site, BAND=band, today=today)
    print(query)

    response = run_query(query, athena_database, s3_output_bucket, region_name)
    download_filename = download_s3_file(response, s3_bucket, region_name, download_local_path)

    print("download file size: ", os.path.getsize(download_filename))
    return download_filename


def load_partitions(env, table):
    """

    shell command:
        export AWS_DEFAULT_REGION=us-east-1
        aws athena start-query-execution --query-string "MSCK REPAIR TABLE secorapp_production.cv_ap_scans" --result-configuration "OutputLocation=s3://mist-production-athena/results/"
    :param env:
    :param table:
    :return:
    """
    s3_bucket = 'mist-{ENV}-athena'.format(ENV=env)
    athena_database = 'secorapp_{ENV}'.format(ENV=env)
    table = 'cv_ap_scans'
    region_name = 'us-east-1'

    s3_output_bucket = 's3://%s/results/' % s3_bucket
    print("s3_output=", s3_output_bucket)

    query = '''MSCK REPAIR TABLE "secorapp_{ENV}"."{TABLE}"'; '''.format(ENV=env, TABLE=table)
    print(query)

    response = run_query(query, athena_database, s3_output_bucket, region_name)
    pass


def test_file():
    # from s3_athena_api import *
    env = "production"
    site = 'a40ba08c-5e27-40d9-b798-7020ca6e4cae'
    band = "5"
    download_file = get_ap_scan(env, site, band)
    print("download file=", download_file)

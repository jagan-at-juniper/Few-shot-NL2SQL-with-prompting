import binascii
import argparse
import hashlib
import time
import random
import textwrap
from util.config import AppConfig, Parameter
import util.converter as cvt
from util.common import datetime_str_to_epoch
from util.logger import get_logger
from cloud.aws_services.emr import Livy
from cloud.factory import CloudProviderFactory

logger = get_logger('data-replay')
8998


# importing the requests library
import requests

# defining the api-endpoint
API_ENDPOINT = "http://pastebin.com/api/api_post.php"

# your API key here
API_KEY = "XXXXXXXXXXXXXXXXX"

# your source code here
source_code = ''' 
print("Hello, world!") 
a = 1 
b = 2 
print(a + b) 
'''

# data to be sent to api
data = { "className": "org.apache.spark.examples.SparkPi",
         "pyFiles":["a.py"],
         "files":["foo.txt"],
         "executorMemory": "20g",
         "args": [2000],
         "file": "/path/to/examples.jar",
         }

# sending post request and saving response as response object
r = requests.post(url=API_ENDPOINT, data=data)

# extracting response text
pastebin_url = r.text
print("The pastebin URL is:%s" % pastebin_url)


class ReplayData(object):
    def __init__(self, params):
        self._env = params['env'].upper()
        self._start = params['start']
        self.job_type = params['type']
        self._params = params
        self._run_time = params['replay']
        self._cloud_provider = CloudProviderFactory.get_provider(params['provider'], self._env)
        self.source_bucket = self._app_config[self._env]['OBJECT_STORE_SOURCE_BUCKET']
        self.source_base_path = self._app_config[self._env]['OBJECT_STORE_SOURCE_BASE_PATH']
        self.file_name = '{}'.format(hashlib.sha256(
            '-'.join([str(self._start), str(self._end), self._mac]).encode('utf-8'))
                                .hexdigest())

    @staticmethod
    def _decode_byte_columns(data):
        uuid_decode_fields = ['org_id', 'site_id']
        for c in uuid_decode_fields:
            data[c] = data.apply(lambda r: cvt.convert_byte_to_uuid(r[c]), axis=1)
        return data

    def get_running_cluster_host(self, name_like='data-science-dev'):
        clusters = self._cloud_provider.etl_list_clusters(['WAITING'])
        clusters = [c for c in clusters if name_like in c.get('Name')]
        if clusters:  # TODO improve selection logic
            cluster = self._cloud_provider.get_emr_cluster_info(
                clusters[random.randint(0, len(clusters) - 1)].get('Id')
            )
            return cluster.get('Cluster').get('MasterPublicDnsName'), \
                cluster.get('Cluster').get('Id'), cluster.get('Cluster').get('Name')
        return None

    def livy_spark_extract(self, sync=True):  # TODO GCP implementation
        logger.info("Preparing for Spark data extraction - Livy")
        file_key = '{}/{}'.format(self.app_base_path, self.file_name)
        if not self._cloud_provider.object_store_prefix_exists(self.app_bucket, file_key):
            cluster = self.get_running_cluster_host()
            if not cluster:
                logger.info('No cluster available now, Exiting ...')
                return {"status": "error", 'msg': 'No cluster available'}
            cluster_host = 'http://{}'.format(cluster[0])
            logger.info("CLUSTER ID: {} Name: {} HOST: {}".format(cluster[1], cluster[2], cluster[0]))

            with open('event_replay/spark_job/extract_data.py', 'r') as f:
                template = f.read()
            if len(template):
                template_extra = """
                in_base_path = '{}'
                """.format(
                    "s3://{}/{}".format(self.source_bucket, self.source_base_path)
                )

                template += textwrap.dedent(template_extra)
                code = {
                    'code': textwrap.dedent(template)
                }
                livy = Livy(cluster_host)
                resp = livy.submit_statement(code)
                if isinstance(resp, dict) and 'id' in resp.keys():
                    if sync:
                        status = livy.get_statement_status_with_retry(resp.get('id'))
                    else:
                        status = livy.get_statement_status(resp.get('id'))
                    try:
                        msg = status.get('output').get('evalue')
                    except KeyError as e:
                        msg = 'Job complete'
                    return {'status': status.get('output').get('status').capitalize(),
                            'msg': msg}
                else:
                    return {'status': 'error',
                            'msg': 'Unable to submit the job to cluster'}
        else:
            logger.info('Data extract exist {}'.format(file_key))
            return {"status": "Exists", 'msg': 'data extract {}'.format(file_key)}

    def run_job(self):
        job_types = {
            'e': 'Extract',
            's': 'Show',
            'r': 'Replay'
        }
        if isinstance(self.job_type, str) and self.job_type in job_types.keys():  # extract, show, replay
            logger.info('JOB TYPE : {}'.format(job_types.get(self.job_type, 'UNKNOWN')))
            logger.info('Environment : {}'.format(self._env))
            if self.job_type == 'e':
                status = self.livy_spark_extract()
            elif self.job_type == 's':
                status, df = self.get_sample_data()
            elif self.job_type == 'r':
                status = self.replay_kafka_data()
            else:
                raise ValueError('Invalid job tpe')
            if status.get('status') == 'error':
                logger.error("{} {}".format(status.get('status'), status.get('msg')))
            else:
                logger.info("{} {}".format(status.get('status'), status.get('msg')))
        else:
            raise ValueError('Invalid job tpe')



if __name__ == '__main__':
    argparser = argparse.ArgumentParser("Replay data")
    args = vars(argparser.parse_args())
    rp = ReplayData(args)
    rp.run_job()


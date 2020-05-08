import uuid
import json
from typing import Dict
import requests
import time
from requests.exceptions import RequestException
from tenacity import retry, wait_fixed, wait_random, stop_after_delay



class EMRSparkStep(object):
    def __init__(self, env, main, py_files, args=None,
                 files=None, extra_spark_args=dict(), **kwargs):
        self._env = env
        self._main = main
        self._py_files = py_files
        self._args = args
        self._files = files
        self._extra_spark_args = extra_spark_args
        self._extra_params = kwargs
        self._action_on_failure = \
            self._extra_params.get('action_on_failure', 'CONTINUE')

    @property
    def name(self):
        return '{}_{}_{}'.format(self.main,
                                 self._extra_params.get('name_suffix'),
                                 uuid.uuid1())

    def build(self):
        """
        spark_yarn_max_attempts
        spark_driver_mem_overhead
        spark_packages
        spark_properties
        :return:
        """
        spark_args = [
            "spark-submit",
            "--deploy-mode", self._extra_spark_args.get('spark_deploy_mode', 'cluster'),
            "--master", self._extra_spark_args.get('spark_master', 'yarn'),
            "--name", self.name,
            "--py-files", self._py_files,
            "--conf", "spark.yarn.maxAppAttempts={}".format(
                self._extra_spark_args('spark_yarn_max_attempts', 1)
            ),
                      ]

        if self._extra_spark_args('spark_driver_mem_overhead'):
            spark_args += [
                "--conf",
                "spark.driver.memoryOverhead={}m".format(
                    self._extra_spark_args('spark_driver_mem_overhead')
                )
            ]

        # spark packages
        if self._extra_spark_args('spark_packages') and \
                isinstance(self._extra_spark_args('spark_packages'), list):
            spark_args += ['--packages',
                           ','.join(self._extra_spark_args('spark_packages'))]

        # properties
        if self._extra_spark_args('spark_properties') and \
                isinstance(self._extra_spark_args('spark_properties'), dict):
            for prop in self._extra_spark_args('spark_properties'):
                spark_args += ['--conf',
                               '%s=%s'.format(
                                   prop,
                                   self._extra_spark_args(
                                       'spark_properties'
                                   )[prop])]

        # files
        if self._files and isinstance(self._files, list):
            for file in self.files:
                spark_args += ['--files', file]

        # job args
        if self._args:
            spark_args += self._args

        emr_step = {
            "Name": self.name,
            "ActionOnFailure": self._action_on_failure,
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": spark_args,
            }
        }
        return emr_step


class EMRException(Exception):
    pass


class Livy:
    def __init__(self, host: str, kind: str = 'pyspark', port: int = 8998):
        self._url: str = '{}:{}'.format(host, port)
        self._headers: Dict = {'Content-Type': 'application/json'}
        self._kind: str = kind
        self.__session_url: str = None
        self.__statement_url: str = None
        self._session_url = self._get_session()

    def _get_session(self):
        if not self.__session_url:
            try:
                sessions = requests.get(self._url + '/sessions',
                                        headers=self._headers).json()
                if sessions.get('total') and sessions.get('total') > 0:
                    self.__session_url = "{}/sessions/{}".format(
                        self._url, sessions.get('sessions')[0].get('id')  # TODO add heuristic to choose
                    )
                else:
                    res = requests.post(self._url + '/sessions',
                                        data=json.dumps({'kind': self._kind}),
                                        headers=self._headers)
                    self.__session_url = self._url + res.headers['location']
                retry_count = 5
                for c in range(retry_count):
                    time.sleep(20)  # TODO get timeout params
                    r = requests.get(self.__session_url, headers=self._headers).json()
                    if r['state'] == 'starting':
                        continue
                    break
            except RequestException as e:
                print(e)
        return self.__session_url

    @property
    def _statement_url(self):
        if not self.__statement_url:
            return '{}/statements'.format(self._session_url)
        return self.__statement_url

    def submit_statement(self, data: Dict):
        try:

            return requests.post(self._statement_url,
                                 data=json.dumps(data),
                                 headers=self._headers).json()
        except RequestException as e:
            print(e)
            return False

    def get_statement_status(self, statement_id: str):

        try:
            return requests.get(
                '{}/{}'.format(self._statement_url, statement_id),
                headers=self._headers).json()
        except RequestException as e:
            print(e)
            pass

    @retry(wait=wait_fixed(15) + wait_random(20), stop=stop_after_delay(600))  # Stop after 10min
    def get_statement_status_with_retry(self, statement_id: str):
        resp = requests.get(
            '{}/{}'.format(self._statement_url, statement_id),
            headers=self._headers).json()
        logger.info('PROGRESS: {}% '.format(round(resp.get('progress')*100)))
        if resp.get('state') == 'available':
            return resp
        raise RequestException()

    def delete_session(self):
        if self.__session_url:
            try:
                res = requests.delete(self.__session_url,
                                      headers=self._headers).json()
                if res.get('msg') and res.get('msg') == 'deleted':
                    return True
            except RequestException as e:
                logger.error(e)
        return False

    def __del__(self):
        self.delete_session()




from emr import EMRSparkStep, Livy


# def __init__(self, env, main, py_files, args=None,
#              files=None, extra_spark_args=dict(), **kwargs)
# EMRSparkStep("data-science-staging", )

# data to be sent to api
## file being the main Python executable, and pyFiles being the additional internal libraries that are being used.

# "className": "org.apache.spark.examples.SparkPi",
data = { 'kind': 'pyspark',
        "file":"s3://mist-data-science-dev/ryanr/action_report.py",
         "args": "2020-05-02",
         "name":'action script'}

data_science_dev_4 = "http://ec2-3-93-213-4.compute-1.amazonaws.com"

livy = Livy(data_science_dev_4)
request = livy.submit_statement(data)
print(request)


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
            out_file_name = '{}'
            s = {}
            e = {}
            mac_id = '{}'
            bucket = '{}'
            key = '{}' 
            extract_data(spark, in_base_path,out_file_name, s, e, mac_id, bucket, key)
            """.format(
                "s3://{}/{}".format(self.source_bucket, self.source_base_path),
                "s3://{}/{}".format(self.app_bucket, file_key),
                int(self._start), int(self._end),
                self._mac, self.app_bucket, file_key
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
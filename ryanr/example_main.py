#!/usr/bin/env python
import argparse
import importlib
import time
import os
import logging
import sys
# sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# from import data_transform.util.spark.figure_spark_session import get_spark_session_instance

####### Note : this is just reference code copied from my previous spark jobs ############

if __name__ == '__main__':
    """
    call spark-submit with --jars gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar 
    """
    parser = argparse.ArgumentParser(description='run a mist pyspark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name',
                        help="The name of the job module you want to run. (ex: load_mailing_file will run job from "
                             "jobs.load_mailing_file)")
    parser.add_argument('--job-args', nargs='*',
                        help="Add'l arguments to send to the spark job "
                             "(example: --job-args run_date=20170717 foo=bar")
    args = parser.parse_args()
    logging.info("Called with arguments: {}".format(args))
    environ = {
        'PYSPARK_JOB_ARGS': ' '.join(args.job_args) if args.job_args else ''
    }
    job_args = dict()
    if args.job_args:
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]
        logging.info('job_args_tuples: {}'.format(job_args_tuples))
        job_args = {a[0]: a[1] for a in job_args_tuples}
    logging.info(f'\nRunning job {args.job_name}...\nenvironment is {environ}\n')
    os.environ.update(environ)
    app_name = args.job_name
    spark = get_spark_session_instance(app_name)
    spark.sparkContext.environment = environ

    """
    import the module we want to run.  this can either be a py file or a package, but should be somewhere under
    local file.jobs.
    *** it also needs to implement a run_jobs method*** 
    """
    job_module = importlib.import_module('data_transform.jobs.%s' % args.job_name)
    start = time.time()
    job_module.run_job(spark, **job_args)
    end = time.time()
    logging.info("***** Execution of job {} took {} seconds ******".format(args.job_name, end - start))
    spark.stop()
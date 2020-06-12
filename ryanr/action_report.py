from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import explode, split
from datetime import datetime, timedelta
import argparse

def validate(date):
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")

def get_run_date():
    try:
        parser = argparse.ArgumentParser(description='Optional app description')
        parser.add_argument('pos_arg', type=str, help='A required integer positional argument')
        args = parser.parse_args()
        validate(args.pos_arg)
        date = args.pos_arg
        print("Parsed input date, date format looks correct")
        return date
    except:
        print("Excepting input arg error by setting run date equal to yesterdays")
        d = datetime.today() - timedelta(days=1)
        date = d.strftime('%Y-%m-%d')
        return date
    finally:
        return date

def flatten_action_data (rdd_action_data):
    """
    :param base_action_rdd:
    :return: dataframe ['action', 'ap_id', 'org_name', 'event_name', 'event_type']
    """

    df_action_data = rdd_action_data.map(lambda r: json.loads(r[1]))\
        .map(lambda r:r[0]).filter(lambda r:r['action']!= None)\
        .filter(lambda r:r['action']!= "do_nothing")\
        .toDF()

    df_action_data_cols = df_action_data\
        .select(df_action_data['action'],
                df_action_data['display_entity_id'].alias('ap_id'),
                df_action_data['org_id'],
                explode(df_action_data['event_id']).alias('event_id'))

    split_col = split(df_action_data_cols['event_id'], '&')
    df_with_event_name = df_action_data_cols\
        .withColumn('event_name', split_col.getItem(1))\
        .drop('event_id')

    return df_with_event_name

def join_action_df_w_org_df(df_with_event_name, df_org_id_name_map):

    df_action_w_org = df_with_event_name\
        .join(df_org_id_name_map,df_with_event_name.org_id == df_org_id_name_map.id, 'inner')\
        .drop('created_time','modified_time','msp_id','tzoffset','secret','loaded_date', 'id', 'org_id')

    return df_action_w_org

def save_file_to_s3_action_reports(action_dataframe, date):
    """
    :param dataframe: final action dataframe ready to save
    :param date: format
    :return:
    """
    try:
        action_dataframe.coalesce(1)\
            .write\
            .option("header","true")\
            .option("sep",",")\
            .mode("overwrite")\
            .csv(f"s3://mist-data-science-dev/automated_action_report/{date}/")
    except IOError as err:
        print("I/O error: {0}".format(err))

if __name__ == '__main__':

    """
    import the module we want to run.  this can either be a py file or a package, but should be somewhere under
    local file.jobs.
    *** it also needs to implement a run_jobs method*** 
    """

    date = get_run_date()

    conf = SparkConf().setAppName(f" Spark Submit - Entity Action Report For Date: {date}")
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc)

    rdd_action_data = sc.sequenceFile(f"s3://mist-aggregated-stats-production/entity_action/"
                                      f"entity_action-production/dt={date}/hr=*/AutoAction_*.seq")

    df_org_id_name_map = spark.read.load("s3://mist-secorapp-production/dimension/org/part-*.parquet")\
        .withColumnRenamed("name","org_name")

    action_df = flatten_action_data(rdd_action_data)
    action_w_org_df = join_action_df_w_org_df(action_df, df_org_id_name_map)
    save_file_to_s3_action_reports(action_w_org_df, date)
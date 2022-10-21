import numpy as np
from numpy import *

from pyspark.sql import functions as F

# pyspark functions
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType
from pyspark.sql.types import DoubleType, FloatType, ArrayType, MapType
from pyspark.sql.functions import when, lit, create_map, desc
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import json_tuple, from_json, get_json_object
from pyspark.sql.functions import col, upper, initcap
from pyspark.sql.functions import udf, size, avg, count, sum, explode
from pyspark.sql.functions import expr, collect_list, collect_set
from pyspark.sql.functions import to_timestamp, percentile_approx

from pyspark.ml.feature import StringIndexer

from functools import reduce
from operator import add

from pyspark.sql.functions import year, month, dayofmonth, dayofweek, date_format
import statsmodels.api as sm

# import time
from time import mktime
from datetime import date, timedelta
from typing import List



def find_sundays_between(start: date, end: date) -> List[date]:
    total_days: int = (end - start).days + 1
    sunday: int = 6
    all_days = [start + timedelta(days=day) for day in range(total_days)]
    return [day for day in all_days if day.weekday() is sunday]


def find_weekdays_between(start: date, end: date, week_day) -> List[date]:
    total_days: int = (end - start).days + 1
    all_days = [start + timedelta(days=day) for day in range(total_days)]
    return [day for day in all_days if day.weekday() is week_day]


DEC_PRE = 3

# original column names
ORG_ID = 'org_id'
SITE_ID = 'site_id'
AP_ID = 'ap_id'
ACTIVE_CLIENT_COUNT = 'active_client_count'
TIME = 'time'

# added column names
# time columns
WEEK_IN_DATA = 'week_in_data'
HOUR_OF_WEEK = 'hour_of_week'
TIMESTAMP = "timestamp"
DAY = 'day'
HOUR = 'hour'
DAY_OF_WEEK = 'day_of_week'
DAY_NAME = 'day_name'
MODEL_DAY_OF_WEEK = 'model_day_of_week'   # day of week adjusted to start of training period
MODEL_HOUR_OF_WEEK = 'model_hour_of_week'  # hour of week adjusted to start of training period
WEEK_OF_DATA_LOAD = 'week'   # week in loaded data for training/validation split
LOCAL = 'local_'

# count columns
ACTIVE_CLIENTS = 'active_clients'  # column name for window aggregation for active client counts
ACTIVE_CLIENTS_MAX = 'active_client_max'
ACTIVE_CLIENTS_MIN = 'active_client_min'
AGG_HOUR_WINDOW = 'agg_hour_window'
ACTIVE_CLIENT_RANGE = 'active_client_range'
LULL_THRESHOLD = 'lull_threshold'
LULL_TIME = 'lull_time'
LULL = 'lull'
FIRST_WEEKDAY = 'first_weekday'
VALIDATION_PARM = 'mod_param'

ACTIVE_CLIENTS_MEAN = 'active_clients_mean'
AGGREGATION_TIME = 'aggregation_time'
TIMEZONE = 'timezone'
BEST_HOUR = 'best_hour'
LULL_FREQ = 'lull_frequency'

# MODEL PARAMETERS

BEST_LOCAL_HOUR = 3 # Parameter for rule based selection of the best hour
LULL_FREQUENCY_THRESHOLD = 0.1
LULL_THRESHOLD_PARAM = 0.01   # defines percentage of range above minimum client count for the day
SOFT_LULL_THRESHOLD_PARAM = 0.05

class MaintenanceRecommender():
    """
    Forecast lull (quiet) time in client activity for AP maintenance
    """

    def __init__(self, config, logger=None, broadcast_env_config=None, stats_accu=None):
        """
        Forecast lull (quiet) time in client activity for AP maintenance
        :param config:
        :param logger:
        :param broadcast_env_config:
        """
        self.bucket = "s3://mist-aggregated-stats-production/aggregated-stats/ap_and_client_count_each_site/dt="
        self.time_zone_path = "s3://mist-secorapp-production/site-timezone/site-timezone-production/dt="
        self.output_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/site_maintenance_window/"
        self.output_bucket_model = 's3://mist-aggregated-stats-production/aggregated-stats/site_maintenance_window_model/'
        self.validation_week = 'week_5'
        self.debug = True
        self.lull_freq_th = config.get('lull_freq_th', LULL_FREQUENCY_THRESHOLD)
        self.thresold_param = config.get('lull_thresold_param', LULL_THRESHOLD_PARAM)

    def load_data(self, collection_day, duration=28):
        """
        Place holder for data load. Need to update spark_jobs/analytics/jobs/stats_aggregator.py
        to do daily aggregations 
        :param collection_day: run day for aggregation '2022-8-15'  string
              code will run starting from the previous day
        :param duration: number of days in the past 7 * n  default = 28, 4 weeks
        :return:
        """
        model_start = date(int(collection_day[:4]), int(collection_day[5:7]), int(collection_day[8:10]))
        # organise data for training
        # last day of training is the day before day of model
        training_end = model_start - timedelta(days=1)
        training_start = model_start - timedelta(days=duration)
        print('<=== Training starting date:')
        print(training_start)
        print('<=== Training ending date:')
        print(training_end)
        training_start_day_ot_week = training_start.weekday()  # NOTE SCHEMA FOR DAYS HERE {'M': 0, 'T':1 }
        training_week_starts = find_weekdays_between(training_start, training_end, training_start_day_ot_week)

        weeks = {}
        for n, day_in in enumerate(training_week_starts):
            all_days = [day_in + timedelta(days=d) for d in range(7)]
            weeks['week_' + str(n)] = [str(x) for x in all_days]

        self.validation_week = 'week_' + str(len(training_week_starts) - 1)
        first_week = weeks['week_0']
        first_day = first_week[0]
        file_in = self.bucket + first_day + '/hr=*/'
        df_out = spark.read.parquet(file_in)
        df_out = df_out.withColumn(WEEK_OF_DATA_LOAD, F.lit('week_0'))
        df_time_zone = self.add_time_zone(first_day)
        df_out = df_out.join(df_time_zone, 'site_id')
        for week in weeks:
            for day in weeks[week]:
                if day != first_day:
                    file_in = self.bucket + day + '/hr=*/'
                    df = spark.read.parquet(file_in)
                    df = df.withColumn(WEEK_OF_DATA_LOAD, F.lit(week))
                    df_time_zone = self.add_time_zone(day)
                    df = df.join(df_time_zone, 'site_id')
                    df_out = df_out.unionAll(df)
        return df_out

    def add_time_zone(self, day):
        """
        Read time zone info for all sites. File format "orc"
        :param day:
        :return:
        """
        file_in = self.time_zone_path + day + '/'
        df = spark.read.format('orc').load(file_in)
        df = df.select(['site_id', TIMEZONE])
        return df

    def get_mean(self, df, entity_cols, count_col, output_col, filter_in=""):
        """
        Calculate mean
        :param df:  data frame
        :param entity_cols:  list of aggregation entity example: ['org_id', 'site_id', 'ap_mac']
        :param count_col:  columns to estimate quantiles
        :param output_col:  output column name for mean
        :param filter_in:  Filtering condition if required example: "F.col('count_roaming')!=0"
        :return: df with added columns
        """
        if filter_in:
            df = df.withColumn("temp", F.when(eval(filter_in), F.col(count_col)).otherwise(None))
            win = Window.partitionBy(entity_cols).orderBy(F.col(count_col))
            df = df.withColumn("temp_out", F.avg(F.col("temp")).over(win))
            df = df.drop(*["temp"])
        else:
            win = Window.partitionBy(entity_cols).orderBy(F.col(count_col))
            df = df.withColumn("temp_out", F.avg(F.col(count_col)).over(win))
        win2 = Window.partitionBy(entity_cols).orderBy(F.col(count_col).desc())
        df = df.withColumn(output_col, F.round(F.first("temp_out").over(win2), DEC_PRE))
        df = df.drop(*["temp_out"])
        return df

    def pre_process_data(self,
                             df,
                             time_col,
                             collection_day,
                             duration,
                             add_week_in_data=True):
        """
        ADD information on day of the week, hour of the week etc. based on timestamp
        add local timing based on site local
        :param df:
        :param time_col:
        :param collection_day: first day of the week in data with standard enumeration: {sun:1, M: 2,...sat:7}
        :param duration:
        :param add_week_in_data: Needed for aggregation
        :return:
        """
        df = df.withColumn(TIMESTAMP, F.to_timestamp(F.col(time_col))).persist()
        df = df.withColumn(DAY, F.to_date(F.col(TIMESTAMP)))
        df = df.withColumn(HOUR, F.hour(TIMESTAMP))
        df = df.withColumn(DAY_OF_WEEK, F.dayofweek(TIMESTAMP))   # from 1 for Sun to to 7 for Sat
        df = df.withColumn(DAY_NAME, F.date_format(DAY, 'E'))
        df = df.withColumn(HOUR_OF_WEEK, (F.col(DAY_OF_WEEK) - 1) * 24 + F.col(HOUR))

        model_start = date(int(collection_day[:4]), int(collection_day[5:7]), int(collection_day[8:10]))
        training_start = model_start - timedelta(days=duration)
        training_start_day_ot_week = int((training_start.weekday() - 6) % 7 + 1)  # adapt for spark
        starting_timestamp = int(mktime(model_start.timetuple()))

        if add_week_in_data:   # needed in case of validation
            date_cols = [ORG_ID, SITE_ID]
            df = df.withColumn('temp', F.when((F.col(DAY_OF_WEEK) == training_start_day_ot_week) &
                                              (F.col(HOUR) == 0), 1).otherwise(0))
            win1 = Window.partitionBy(date_cols).orderBy(F.col(TIMESTAMP))
            df = df.withColumn(WEEK_IN_DATA, F.sum(F.col('temp')).over(win1))
            df = df.drop(*['temp'])

        df = df.withColumn(AGGREGATION_TIME, F.lit(starting_timestamp).cast(LongType()))
        df = df.withColumn(MODEL_DAY_OF_WEEK, F.exp(F.col(DAY_OF_WEEK) + 1 - training_start_day_ot_week % 7))
        df = df.withColumn(MODEL_DAY_OF_WEEK, F.round(F.col(MODEL_DAY_OF_WEEK) + 1, 1).cast(IntegerType()))
        df = df.withColumn(MODEL_HOUR_OF_WEEK, (F.col(MODEL_DAY_OF_WEEK) - 1) * 24)
        df = df.withColumn(MODEL_HOUR_OF_WEEK, (F.col(MODEL_HOUR_OF_WEEK) + F.col(HOUR)).cast(IntegerType()))

        # add local time + hour + day + day of the week
        df = df.withColumn(LOCAL + TIMESTAMP,
                           F.from_utc_timestamp(F.col(TIMESTAMP), F.col(TIMEZONE)))
        df = df.withColumn(LOCAL + DAY, F.to_date(F.col(LOCAL + TIMESTAMP)))
        df = df.withColumn(LOCAL + HOUR, F.hour(LOCAL + TIMESTAMP))
        df = df.withColumn(LOCAL + DAY_OF_WEEK, F.dayofweek(LOCAL + TIMESTAMP))  # Sundays start at 1
        df = df.withColumn(LOCAL + DAY_NAME, F.date_format(LOCAL + DAY, 'E'))

        return df

    def aggregate(self,
                  df,
                  hours=HOUR_OF_WEEK,
                  max_hours=168,
                  agg_period=WEEK_IN_DATA,
                  window=0,
                  method='max'):
        """
        aggregate values within windows and creates
        ACTIVE_CLIENTS = ACTIVE_CLIENT_COUNT * AP_ID
        Steps:
        1. pad data or cycle df
        2. sliding window with aggregate max, sum, or other function
        :param df:
        :param hours: column 'hour_of_week' or 'hour' if data has only daily, not weekly periodicity
        :param max_hours: duration of interval 168 for hours of the week and 24 for days
        :param agg_period: 'week_in_data' or 'day'
        :param window: duration of recommendation interval 1 hour, 2 hours, etc.
        :param method: agg function within time window: ['max', 'mean', 'sum']
        :return:
        """
        # 1. pad df to account for joined windows

        if window > 0:
            hours_col = hours
            if agg_period == WEEK_IN_DATA:
                hours_col = MODEL_HOUR_OF_WEEK

            df_pad = df.where(F.col(hours_col) < window
                              ).withColumn(hours_col,
                                           F.col(hours_col) + max_hours)
            df = df.unionAll(df_pad)

            # 2. window aggregate
            data_cols = [ORG_ID, SITE_ID, agg_period]
            win = Window().partitionBy(data_cols).orderBy(F.col(hours_col)).rowsBetween(-window, 0)
            if method == 'max':
                df = df.withColumn(ACTIVE_CLIENTS,
                                   F.max(F.col(ACTIVE_CLIENT_COUNT) * F.col(AP_ID)).over(win))
            elif method == 'mean':
                df = df.withColumn(ACTIVE_CLIENTS,
                                   F.round(F.avg(F.col(ACTIVE_CLIENT_COUNT) * F.col(AP_ID)).over(win), DEC_PRE))
            elif method == 'sum':
                df = df.withColumn(ACTIVE_CLIENTS,
                                   F.sum(F.col(ACTIVE_CLIENT_COUNT) * F.col(AP_ID)).over(win))
            else:  # use 'max' as default
                df = df.withColumn(ACTIVE_CLIENTS,
                                   F.max(F.col(ACTIVE_CLIENT_COUNT) * F.col(AP_ID)).over(win))
            df = df.where(F.col(hours_col) < max_hours)

        else:   # hourly data
            df = df.withColumn(ACTIVE_CLIENTS, F.col(ACTIVE_CLIENT_COUNT) * F.col(AP_ID))

        df = df.withColumn(AGG_HOUR_WINDOW, F.lit(window))
        return df

    def add_lull(self, df, value_col, thresold_param=0.01, lull_range=DAY):
        """
        define max/min range for aggregated data and add "lull" based on a threshold
        :param df: data frame
        :param value_col: value column:  'active_client_count' or 'active_clients' for agg
        :param thresold_param: threshold to define lull: lull < min + percent*(max -min)
        :param lull_range: time interval to seasrch for lull: daily: 'day' or week: 'week_in_data'
        """
        #  get max during the day
        data_cols = [ORG_ID, SITE_ID, lull_range]
        win1 = Window.partitionBy(data_cols).orderBy(F.col(value_col).desc())
        df = df.withColumn(ACTIVE_CLIENTS_MAX, F.round(F.first(F.col(value_col)).over(win1), DEC_PRE))
        #  get min during the day
        win2 = Window.partitionBy(data_cols).orderBy(F.col(value_col))
        df = df.withColumn(ACTIVE_CLIENTS_MIN, F.round(F.first(F.col(value_col)).over(win2), DEC_PRE))

        # define threshold:  d = get 1% or 5% of max-min  (set as parameter for evaluation)
        df = df.withColumn(ACTIVE_CLIENT_RANGE, F.col(ACTIVE_CLIENTS_MAX) - F.col(ACTIVE_CLIENTS_MIN))
        df = df.withColumn(LULL_THRESHOLD, F.col(ACTIVE_CLIENTS_MIN) + thresold_param * F.col(ACTIVE_CLIENT_RANGE))

        # Check lull times per day
        df = df.withColumn(LULL_TIME, F.when(F.col(value_col) < F.col(LULL_THRESHOLD), 1).otherwise(0))
        return df

    def get_lull_model(self, df, value_col, thresold_param=0.01, lull_range=DAY, seasonality='both'):
        """
        Define frequency of lull based on historical data following weekly or hourly seasonality
        :param df: data frame
        :param value_col:  'active_clients'
        :param thresold_param: threshold to define lull: lull < min + percent*(max -min)
        :param lull_range: interval column for lull detection daily: 'day' (not implemented or weekly: 'week_in_data')
        :param seasonality: week  or day
        """
        df = self.add_lull(df, value_col, thresold_param, lull_range)
        output_columns_d = [value_col + '_mean_d',
                            LULL_FREQ + '_d',
                            ACTIVE_CLIENTS_MAX + '_av_d',
                            ACTIVE_CLIENTS_MIN + '_av_d']
        output_columns_w = [value_col + '_mean_w',
                            LULL_FREQ + '_w',
                            ACTIVE_CLIENTS_MAX + '_av_w',
                            ACTIVE_CLIENTS_MIN + '_av_w']
        output_columns = [value_col + '_mean',
                          LULL_FREQ,
                          ACTIVE_CLIENTS_MAX + '_av',
                          ACTIVE_CLIENTS_MIN + '_av']

        if seasonality == 'week':
            # Check number of times during last weeks the time interval was a "lull"
            data_cols = [ORG_ID, SITE_ID, DAY_OF_WEEK, HOUR]
            df = self.get_mean(df, data_cols, value_col, value_col + '_mean')
            df = self.get_mean(df, data_cols, LULL_TIME, LULL_FREQ )

            data_cols2 = [ORG_ID, SITE_ID, DAY_OF_WEEK]
            df = self.get_mean(df, data_cols2, ACTIVE_CLIENTS_MAX, ACTIVE_CLIENTS_MAX + '_av')
            df = self.get_mean(df, data_cols2, ACTIVE_CLIENTS_MIN, ACTIVE_CLIENTS_MIN + '_av')
        elif seasonality == 'day':
            # Check number of times during last weeks the time interval was a "lull"
            data_cols = [ORG_ID, SITE_ID, HOUR]
            df = self.get_mean(df, data_cols, value_col, value_col + '_mean')
            df = self.get_mean(df, data_cols, LULL_TIME, LULL_FREQ)

            data_cols2 = [ORG_ID, SITE_ID]
            df = self.get_mean(df, data_cols2, ACTIVE_CLIENTS_MAX, ACTIVE_CLIENTS_MAX + '_av')
            df = self.get_mean(df, data_cols2, ACTIVE_CLIENTS_MIN, ACTIVE_CLIENTS_MIN + '_av')
        else:
            print('<=== Weekly and daily seasonality models ===> ')
            # Check number of times during last weeks the time interval was a "lull"
            data_cols_w = [ORG_ID, SITE_ID, DAY_OF_WEEK, HOUR]
            df = self.get_mean(df, data_cols_w, value_col, value_col + '_mean_w')
            df = self.get_mean(df, data_cols_w, LULL_TIME, LULL_FREQ + '_w')
            data_cols_w_2 = [ORG_ID, SITE_ID, DAY_OF_WEEK]
            df = self.get_mean(df, data_cols_w_2, ACTIVE_CLIENTS_MAX, ACTIVE_CLIENTS_MAX + '_av_w')
            df = self.get_mean(df, data_cols_w_2, ACTIVE_CLIENTS_MIN, ACTIVE_CLIENTS_MIN + '_av_w')

            data_cols = [ORG_ID, SITE_ID, HOUR]
            df = self.get_mean(df, data_cols, value_col, value_col + '_mean_d')
            df = self.get_mean(df, data_cols, LULL_TIME, LULL_FREQ + '_d')
            data_cols2 = [ORG_ID, SITE_ID]
            df = self.get_mean(df, data_cols2, ACTIVE_CLIENTS_MAX, ACTIVE_CLIENTS_MAX + '_av_d')
            df = self.get_mean(df, data_cols2, ACTIVE_CLIENTS_MIN, ACTIVE_CLIENTS_MIN + '_av_d')
            output_columns = output_columns_w + output_columns_d

        select_cols = [ORG_ID,
                       SITE_ID,
                       DAY_OF_WEEK,
                       DAY_NAME,
                       HOUR,
                       HOUR_OF_WEEK,
                       AGG_HOUR_WINDOW,
                       AGGREGATION_TIME,
                       MODEL_DAY_OF_WEEK,
                       TIMEZONE,
                       LOCAL + HOUR,
                       LOCAL + DAY_OF_WEEK,
                       LOCAL + DAY_NAME]
        select_cols = select_cols + output_columns
        df_model = df.select(select_cols).dropDuplicates()
        # Adjust timestamp
        df_model = df_model.withColumn('model_time', F.col(AGGREGATION_TIME) +
                                       ((F.col(MODEL_DAY_OF_WEEK)-1)*24 + F.col(HOUR))*3600)

        return df_model

    def confusion_matrix(self, df, group_cols, test_col, lull_cols):
        """
        Binary fit for hourly vs weekly seasonality
        Calculate precision for each model
        :param df:
        :param group_cols:
        :param test_col:
        :param lull_cols:
        :return:
        """
        df_cc = df.groupBy(group_cols).agg(F.count(F.when(F.col(test_col) == 1, F.col(test_col))).alias('TOTAL_P'),
                                           F.count(F.col(test_col)).alias("TOTAL"),
                                           F.count(F.when((F.col(test_col) == 1) & (F.col('lull_1') == 1),
                                                          F.col('lull_1'))).alias('TP_1'),
                                           F.count(F.when(F.col('lull_1') == 1, F.col('lull_1'))).alias('PP_1'),
                                           F.count(F.when((F.col(test_col) == 1) & (F.col('lull_2') == 1),
                                                          F.col('lull_2'))).alias('TP_2'),
                                           F.count(F.when(F.col('lull_2') == 1, F.col('lull_2'))).alias('PP_2'),
                                            )
        df_cc = df_cc.withColumn('PRECISION_1',
                                 F.when(F.col('PP_1') != 0,
                                        F.round(F.col('TP_1') / F.col('PP_1'), 3)
                                        ).otherwise(0))
        df_cc = df_cc.withColumn('PRECISION_2',
                                 F.when(F.col('PP_2') != 0,
                                        F.round(F.col('TP_2') / F.col('PP_2'), 3)
                                        ).otherwise(0))
        return df_cc

    def train_model(self, df, value_col, thresold_param, lull_range):
        """
        split loaded data into training and validation
        run lull model on training set with 2 seasons day and week
        detect lull in validation
        compare with  weighted models: alpha * lul_freq_w + (1-alpha)
        :param df:
        :param value_col:
        :param thresold_param:
        :param lull_range: 'day'
        :return:
        """
        df_train = df.where(F.col('week') != self.validation_week)
        df_val = df.where(F.col('week') == self.validation_week)
        df_model = self.get_lull_model(df_train, value_col, thresold_param, lull_range, 'both')

        if self.debug:
            print("<=== columns for model  ===>")
            print(df_model.columns)

        df_validate = self.add_lull(df_val, value_col, thresold_param, lull_range)

        if self.debug:
            print("<=== columns for validate ===>")
            print(df_validate.columns)

        join_cols = [ORG_ID,
                     SITE_ID,
                     DAY_OF_WEEK,
                     DAY_NAME,
                     HOUR,
                     HOUR_OF_WEEK,
                     AGG_HOUR_WINDOW,
                     AGGREGATION_TIME,
                     MODEL_DAY_OF_WEEK,
                     TIMEZONE,
                     LOCAL + HOUR,
                     LOCAL + DAY_OF_WEEK,
                     LOCAL + DAY_NAME]

        alpha = {'1': 1, '2': 0}
        model_cols = []
        for a in alpha:
            aa = alpha[a]
            df_model = df_model.withColumn(LULL_FREQ + '_' + a,
                                           aa * F.col(LULL_FREQ + '_w') + (1-aa) * F.col(LULL_FREQ + '_d'))
            df_model = df_model.withColumn(LULL + '_' + a,
                                           F.when(F.col(LULL_FREQ + '_' + a) < thresold_param, 1) .otherwise(0))
            model_cols = model_cols + [LULL + '_' + a]

        df_validate = df_validate.join(df_model, join_cols)
        group_cols = [ORG_ID, SITE_ID]
        test_col = LULL_TIME
        cc = self.confusion_matrix(df_validate, group_cols, test_col, model_cols)

        # find max Precision and column name
        cc = cc.withColumn('PRECISION_max', F.greatest(F.col('PRECISION_1'),
                                                       F.col('PRECISION_2')))

        # get best model
        cc = cc.withColumn(VALIDATION_PARM, F.when(F.col('PRECISION_1') >= F.col('PRECISION_2'), 1).otherwise(0))
        val_output = cc.select([ORG_ID, SITE_ID, VALIDATION_PARM])
        if self.debug:
            print("<=== Validation output sample ===>")
            print(val_output.take(3))

        return val_output

    def get_best_hours(self, df, window_weight_col, lull_freq_col, client_count_rank):
        """
        Select the best hours based on frequency and historical number of clients
        :param df: data frame
        :param window_weight_col:
        :param lull_freq_col:
        :param client_count_rank: historical rank of number of clients
        :return:
        """
        print('<=== GET BEST HOUR AND APPLY RULE FOR MISSING DAYS ===>')
        #  Define best hours
        data_cols_d = [ORG_ID, SITE_ID, DAY_OF_WEEK, DAY_NAME]
        win3 = Window.partitionBy(data_cols_d).orderBy(F.col(window_weight_col))
        df = df.withColumn(window_weight_col + "_min", F.first(window_weight_col).over(win3))
        df = df.withColumn('best_hour', F.when(F.col(window_weight_col) == F.col(window_weight_col + "_min"), 1).otherwise(0))

        # check for multiple hours and choose higher stability or night time
        win3a = Window.partitionBy(data_cols_d + ['best_hour']).orderBy(F.col(lull_freq_col).desc())
        df = df.withColumn('temp_p_best_hours', F.first(F.col(lull_freq_col)).over(win3a))
        df = df.withColumn('best_hour', F.when((F.col('best_hour') == 1) &
                                               (F.col(lull_freq_col) == F.col('temp_p_best_hours')),
                                               1).otherwise(0))

        # count pre-selected best hours per site/day
        win4 = Window.partitionBy(data_cols_d)
        df = df.withColumn('temp_tot_best_hours', F.sum(F.col('best_hour')).over(win4))
        win4a = Window.partitionBy(data_cols_d).orderBy(F.col('temp_tot_best_hours').desc())
        df = df.withColumn('tot_best_hours', F.first(F.col('temp_tot_best_hours')).over(win4a))

        # to keep only 1 hour, sort by hours and choose the first hour of the set
        win5 = Window.partitionBy(data_cols_d).orderBy(F.col(HOUR))
        df = df.withColumn('best_hour_rule', F.sum(F.col('best_hour')).over(win5))
        df = df.withColumn('best_hour', F.when((F.col('tot_best_hours') > 1) &
                                               (F.col('best_hour_rule') != 1),
                                               0).otherwise(F.col('best_hour')))
        df = df.drop('best_hour_rule')
        # for sites with no pre-selected hours
        # choose the best time during local night: BEST_LOCAL_HOUR: 1am- 4am
        # TODO: add based on historical number of clients
        """
        df = df.withColumn('best_hour', F.when((F.col(client_count_rank) == 1) & 
                                               (F.col('tot_best_hours') == 0),
                                               1).otherwise(F.col('best_hour')))
        """

        df = df.withColumn('best_hour', F.when((F.col('tot_best_hours') == 0) &
                                               (F.col(LOCAL + HOUR) == BEST_LOCAL_HOUR),
                                               1).otherwise(F.col('best_hour')))

        df  = df.drop('tot_best_hours')
        return df

    def get_fit(self, df, value_col, lull_freq_th):
        """

        :param df:
        :param value_col:  'active_client_count_mean'  or 'active_clients_mean'
        :param lull_freq_th: threshold on frequency of lull
        :return:
        """
        # Lull candidate based on the threshold

        print('<=== columns before fit the hour ===>')
        print(df.columns)
        df = df.withColumn('lull', F.when(F.col(LULL_FREQ) >= lull_freq_th, 1).otherwise(0))

        # Get ranking for daily client counts and probability of lull
        data_cols_d = [ORG_ID, SITE_ID, DAY_OF_WEEK, DAY_NAME]
        win1 = Window.partitionBy(data_cols_d).orderBy(F.col(value_col))
        df = df.withColumn("count_rank_day", F.dense_rank().over(win1))
        win2 = Window.partitionBy(data_cols_d).orderBy(F.col(LULL_FREQ).desc())
        df = df.withColumn("prob_rank_day", F.dense_rank().over(win2))
        df = df.withColumn('window_weight', F.col("count_rank_day") * F.col("prob_rank_day"))
        df = df.withColumn('window_score', F.when((F.col('window_weight') != 0) &
                                                  (F.col('lull') != 0),
                                                  1 / F.col('window_weight')).otherwise(0))

        # Get ranking for weekly client counts and probability of lull based on daily lulls
        data_cols_w = [ORG_ID, SITE_ID]
        win1w = Window.partitionBy(data_cols_w).orderBy(F.col(value_col))
        df = df.withColumn("count_rank_week", F.dense_rank().over(win1w))
        win2w = Window.partitionBy(data_cols_w).orderBy(F.col(LULL_FREQ).desc())
        df = df.withColumn("prob_rank_week", F.dense_rank().over(win2w))
        df = df.withColumn('window_weight_week', F.col("count_rank_week") * F.col("prob_rank_week"))
        df = df.withColumn('window_score_week', F.when((F.col('window_weight_week') != 0) &
                                                       (F.col('lull') != 0),
                                                       1 / F.col('window_weight_week')).otherwise(0))
        print('<=== GET BEST HOUR  ===>')
        df = self.get_best_hours(df, 'window_weight', LULL_FREQ, "count_rank_week")

        print('<=== columns after fit the hour ===>')
        print(df.columns)

        return df

    def get_data_with_lull(self, date_start, duration, smooth_window=0, method='max'):
        """

        :param date_start:  date of aggregation
        :param duration: duration of past weeks
        :param smooth_window: if aggregation or smoothing is needed 0= no smoothing, 1  - 2 hours etc.
        :param method:  agg function within time window: ['max', 'mean', 'sum']
        :return:
        """
        df = self.load_data(date_start, duration)

        df = self.pre_process_data(df,
                                   TIME,
                                  date_start,
                                  duration)

        # TODO: add hourly aggregation
        # aggregate using weekly period
        df = self.aggregate(df,
                            HOUR_OF_WEEK,
                            168,
                            WEEK_IN_DATA,
                            smooth_window,
                            method)
        value_col = ACTIVE_CLIENTS
        df_lull = self.add_lull(df, value_col, 0.01, DAY)
        return df_lull

    def get_pre_process(self, date_start, duration):
        """

        :param date_start:
        :param date_end:
        :return:
        """
        df = self.load_data(date_start, duration)
        df = self.pre_process_data(df,
                                   TIME,
                                   date_start,
                                   duration)

        return df

    def get_predict(self,
                    df,
                    date_start,
                    duration,
                    smooth_window,
                    lull_range,
                    thresold_param=0.01,
                    method='max',
                    seasonality='week',
                    validation=False):
        """

        :param df: data frame
        :param date_start: date start
        :param duration:
        :param smooth_window: window of smoothing. Default = 0
        :param lull_range: column for lull detection daily: 'day' ( not implemented or weekly: 'week_in_data')
        :param thresold_param:
        :param method:
        :param seasonality: week, day, both
        :param validation:
        :return:
        """

        df = self.pre_process_data(df,
                                   TIME,
                                   date_start,
                                   duration)

        # aggregate if needed
        hours_col = HOUR_OF_WEEK
        max_hours = 168
        agg_period = WEEK_IN_DATA
        method = 'max'
        df = self.aggregate(df,
                            HOUR_OF_WEEK,
                            max_hours,
                            agg_period,
                            smooth_window,
                            method)

        print("<=== Columns before validation ===> ")
        print(df.columns)

        value_col = ACTIVE_CLIENTS
        if validation:
            # Validation is taking too much time now
            # TODO needed more testing
            print("<=== Start required model validation ===> ")
            value_col = ACTIVE_CLIENTS
            model_param = self.train_model(df, value_col, thresold_param, lull_range)
            df_model_day = self.get_lull_model(df, value_col, thresold_param, lull_range, 'both')
            df_model_day = df_model_day.join(model_param, [ORG_ID, SITE_ID])
            output_columns = [value_col + '_mean',
                              LULL_FREQ,
                              ACTIVE_CLIENTS_MAX + '_av',
                              ACTIVE_CLIENTS_MIN + '_av']
            for col_out in output_columns:
                df_model_day = df_model_day.withColumn(col_out,
                                                       F.col(VALIDATION_PARM) * F.col(col_out + '_w') +
                                                       (1 - F.col(VALIDATION_PARM)) * F.col(col_out + '_d'))
                print(df_model_day.columns)
        else:
            print("<=== No model validation is required ===> ")
            print("<=== Start model processing  ===> ")
            df_model_day = self.get_lull_model(df, value_col, thresold_param, lull_range, seasonality)
            if seasonality == 'day':
                df_model_day = df_model_day.withColumn(VALIDATION_PARM, F.lit(0))
            else:
                df_model_day = df_model_day.withColumn(VALIDATION_PARM, F.lit(1))

        print("<=== Columns before fitting ===> ")
        print(df_model_day.columns)
        value_col_fit = ACTIVE_CLIENTS_MEAN
        lull_freq_th = self.lull_freq_th
        df_fit = self.get_fit(df_model_day, value_col_fit, lull_freq_th)
        print("<=== Columns after fitting ===> ")
        print(df_fit.columns)

        return df_fit

    def run_model(self,
                  date_start,
                  duration,
                  smoothing_window,
                  lull_range,
                  thresold_param=0.01,
                  method='max',
                  seasonality='week',
                  validation=False,
                  save_model=True):
        """

        :param date_start:
        :param duration:
        :param smoothing_window:
        :param lull_range:
        :param thresold_param:
        :param method: agg function within time window: ['max', 'mean', 'sum']
        :param seasonality:
        :param validation:
        :param save_model:
        :return:
        """

        df = self.load_data(date_start, duration)
        print('<=== data loaded ===> ')
        print('<=== send to modeling ===>')
        df = self.get_predict(df,
                              date_start,
                              duration,
                              smoothing_window,
                              lull_range,
                              thresold_param,
                              method,
                              seasonality,
                              validation)

        if WEEK_OF_DATA_LOAD in df.columns:
            df = df.drop(WEEK_OF_DATA_LOAD)
        if WEEK_IN_DATA in df.columns:
            df = df.drop(WEEK_IN_DATA)

        # save recommender
        """        
        df.filter('best_hour == 1'). \
            select('site_id', 'day_name', 'hour'). \
            repartition(1).write.option("header", True). \
            partitionBy('day_name').mode("overwrite"). \
            csv('s3://mist-aggregated-stats-production/aggregated-stats/site_maintenance_window/')
        """
        print('<== Saving recommender to ' + self.output_bucket + "  ===>")
        df.filter('best_hour == 1'). \
            select('site_id', 'day_name', 'hour'). \
            repartition(1).write.option("header", True). \
            partitionBy('day_name').mode("overwrite"). \
            csv(self.output_bucket)

        if save_model:
            # output = self.output_bucket + date_start + "/"
            # print('<== Saving complete model to ' + output + "  ===>")
            # df.coalesce(20).write.save(output, format='parquet', mode='overwrite', header=True)
            print('<== Saving complete model to ' + self.output_bucket_model + "  ===>")
            df.repartition(1).write.option("header", True). \
                partitionBy('day_name').mode("overwrite"). \
                parquet(self.output_bucket_model)

        return df

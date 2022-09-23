# Imports
import difflib
import json
import re
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

from analytics.event_generator.event_generator import EventGenerator
from analytics.event_generator.utils import control_java_limit
from analytics.utils.fs_util import compose_file_path_uri
from analytics.utils.fs_util import find_hourly_folders, get_fs_bucket_name

class FailedScopeConfigDiff(EventGenerator):
    """
    detect and generate scope failure events with cross batch aggregated logic based on client events data
    """

    def __init__(self, config, logger=None, broadcast_env_config=None, stats_accu=None):
        super(FailedScopeConfigDiff, self).__init__(config, logger, broadcast_env_config, stats_accu)
        self.input_bucket = self.config.get('input_bucket')
        self.input_prefix = self.config.get('input_prefix')
        self.start_epoch = self.config.get('start_epoch')
        self.logger = logger


    def generate_event(self, data_rdd, spark):
        """
        main func for fetching ap config changes for an AP involved in a scope event
        :return: a list of scope failure events with ap config change information
        """

        self.logger.info("there are totally %d input %s for %s detection" % (
            data_rdd.count(), self.config['entity_type'], self.config['event_name']))

        event_rdd_all = data_rdd
        event_rdd_all = event_rdd_all.map(lambda x: update_event_type(x, self.config['event_type']))

        config_df = self.ap_config_change_info(data_rdd, spark)  # function call to generate dataframe with ap config change details
        # if above dataframe is null, skip rest of the computation
        if not config_df:
            self.logger.info("No config change information on the APs was found for the interval considered")
            return

        config_df.persist()  # persist dataframe
        print('---\n\n\n---', config_df.count())  # can comment out later

        # --- need to add info from ap config change to original rdd
        rdd_config = config_df.rdd.map(lambda x: x.asDict())
        rdd_config_2 = rdd_config.map(lambda x: (x['row_key'], x)).join(event_rdd_all.map(lambda x: (x['row_key'], x)))
        rdd_config_3 = rdd_config_2.map(lambda x: add_config_meta(x))
        out_filename = self.config.get('local_config_bucket_prefix') + str(self.start_epoch) + "_" + str(self.end_epoch) + \
                        self.config.get('local_config_bucket_suffix')
        self.logger.info('Saving appended file with ap config info to %s' % out_filename)
        # print('\n\n\nWriting to file %s' % out_filename)
        rdd_config_3.persist()
        out_text = 'found config change for %d events out of %d scope events' % (rdd_config_2.count(), event_rdd_all.count())
        self.logger.info(out_text)
        # print(out_text)
        # print('\n\n\n----')
        # print(event_rdd_all.take(1))
        # print('\n\n\n----')
        # print(rdd_config_3.take(1))
        # rdd_config_3.map(lambda x: (None, json.dumps(x))).saveAsSequenceFile(out_filename)  #v1.3
        # rdd_config_3.map(lambda x: (None, json.dumps(x))).saveAsSequenceFile(out_filename)  # v1.2 change
        # ---
        config_df.unpersist()
        # event_rdd_all.persist()
        # control the total occurrence within JAVA Int Limit
        # event_rdd_all = event_rdd_all.map(lambda x: control_java_limit(x))
        rdd_config_3 = rdd_config_3.map(lambda x: control_java_limit(x))
        return rdd_config_3

    def ap_config_change_info(self, in_rdd, spark):
        # self.data_source = self.config.get('data_source')
        self.end_epoch = self.config.get('end_epoch')

        # self.logger.info("\n----\ninside new function, event count\n----\n")
        # print(self.data_source, self.end_epoch, self.start_epoch)

        event_rdd_ee_scope = in_rdd.persist()

        # print('\n', event_rdd_ee_scope.count())
        src_bucket = get_fs_bucket_name(bucket_src=self.config.get('source_bucket', 'mist-secorapp'))
        print('>>>> here src_bucket >>>', src_bucket)

        # print("%s \n %s \n %s \n %s \n" % (self.data_source, self.start_epoch, self.end_epoch, src_bucket))  # @di
        # print('in here...')
        # print(type(self.start_epoch), type(self.config.get('config_lookback_before_event')))
        s3loc_secor_apevents_6hr = find_hourly_folders("ap-events", self.start_epoch - self.config.get('config_lookback_before_event'), self.end_epoch, src_bucket)
        # print(">>>> \n\n\n s3loc_secor_apevents_6hr")
        # print(sorted(s3loc_secor_apevents_6hr))

        s3loc_secor_apevents_7days = find_hourly_folders("ap-events", self.start_epoch - self.config.get('previous_config_lookback'),
                                                        self.end_epoch, src_bucket)
        # print(">>>> \n\n\n s3loc_secor_apevents_7days")
        # print(sorted(s3loc_secor_apevents_7days))

        s3loc_secor_apevents_7days_edit = list()
        s3loc_secor_apevents_7days_edit2 = list()
        for x_ in s3loc_secor_apevents_7days:
            if "*" not in x_:
                s3loc_secor_apevents_7days_edit.append(x_)
            else:
                s3loc_secor_apevents_7days_edit2.append(x_)

        print('---\n s3loc_secor_apevents_7days_edit', s3loc_secor_apevents_7days_edit)
        print('---\n s3loc_secor_apevents_7days_edit2', s3loc_secor_apevents_7days_edit2)

        pattern_compile = re.compile('(ap-events/ap-events-production/dt=)(20\d\d-\d\d-\d\d/)(hr=\d\d/)')
        dtlist_7days = set([pattern_compile.search(x_).group(2) for x_ in s3loc_secor_apevents_7days_edit])
        # print(dtlist_7days)

        s3locroot_secor_apevents = compose_file_path_uri(src_bucket) +"/"  # compose_file_path_uri(self.config.get('apstat_config_bucket'))  # Will this require a change for different envs? [TODO]
        print('>>>> here s3locroot_secor_apevents >>>', s3locroot_secor_apevents)

        print('## ap events root location', s3locroot_secor_apevents)
        apfile_locations_6hr = [s3locroot_secor_apevents + x_ for x_ in sorted(s3loc_secor_apevents_6hr)]
        # print('\nThese are files for up to 6 hours ago')
        # for x_ in sorted(apfile_locations_6hr):
        #     print(x_)

        s3locroot_secor_apevents2 = set([pattern_compile.search(x_).group(1) for x_ in s3loc_secor_apevents_7days_edit])
        # print(s3locroot_secor_apevents2)
        pc = re.compile('(ap-events/ap-events-production/dt=)(20\d\d-\d\d-\d\d)')
        if s3loc_secor_apevents_7days_edit2 != []:
            s3locroot_secor_apevents2_1 = [pc.search(x_).group(2) + "/" for x_ in s3loc_secor_apevents_7days_edit2]
            dtlist_7days = set.union(dtlist_7days, s3locroot_secor_apevents2_1)

        ap_filelocations_7days = [s3locroot_secor_apevents + list(s3locroot_secor_apevents2)[0] + x_ + "hr=*/*/" for x_ in
                                 list(dtlist_7days)]

        # # ap_filelocations_7days = [s3locroot_secor_apevents + x_ for x_ in sorted(s3loc_secor_apevents_7days)]
        # print('\nThese are files for up to 7 days ago')
        # for x_ in sorted(ap_filelocations_7days):
        #     print(x_)
        #  ----
        # ---
        pull_schema = T.StructType([
            T.StructField('row_key', T.StringType(), True),
            T.StructField('start_time', T.LongType(), True),
            T.StructField('ap', T.StringType(), True)
        ])
        df_select_scope_events_apscope = event_rdd_ee_scope.map(
            lambda r: (r['row_key'], r['start_time'], r['reason']['ap'])).toDF(pull_schema)

        event_rdd_ee_scope.unpersist()  #code review 1
        df_select_scope_events_apscope.persist()
        print('in here count is %d' % df_select_scope_events_apscope.count())

        ap_list = [x.ap for x in df_select_scope_events_apscope.select('ap').distinct().dropna().collect()]
        # print(len(ap_list))
        # print('Some ap ids: %s', str(ap_list[:10]))


        # pass 1
        outerloopdf = None

        pull_schema2 = T.StructType([
            T.StructField('ap', T.StringType(), True),
            T.StructField('ev_name', T.StringType(), True),
            T.StructField('config', T.MapType(T.StringType(), T.StringType()), True),
            T.StructField('timestamp', T.LongType(), True),
        ])

        read_loc = ",".join(apfile_locations_6hr)
        # for read_loc in apfile_locations_6hr:
        print('Reading 6 hour info...')
        outerloopdf = None
        rdd_apevents = spark.sparkContext.sequenceFile(read_loc).map(lambda r: json.loads(r[1]))  # .persist()
        rdd_apevents_sample = rdd_apevents.filter( lambda r: ('event_type' in r.keys()) and (r['event_type'] == 'device_events') \
                                                            and ('ap' in r['source'].keys()) and (r['source']['ap'] in ap_list) and \
                                                             (r['source']['ev_name'] == "AP_CONFIGURED"))
        # if above rdd returns empty, skip rest of the code run
        rdd_apevents_sample.persist()

        if rdd_apevents_sample.count() > 0:
            outerloopdf = rdd_apevents_sample.map(lambda r: (r['source']['ap'], r['source']['ev_name'],
                                                                    r['source']['config'], r['timestamp'])).toDF(pull_schema2)
            # outerloopdf.persist()
            ap_list_positive = [x.ap for x in outerloopdf.select('ap').distinct().dropna().collect()]
            print(len(ap_list_positive))
            print('Some ap ids: %s', str(ap_list_positive[:10]))

            read_loc2 = ",".join(ap_filelocations_7days)
            print('Reading 7 day info... ')
            outerloopdf_pass2 = None
            rdd_apevents_pass2 = spark.sparkContext.sequenceFile(read_loc2).map(lambda r: json.loads(r[1]))  # .persist()
            rdd_apevents_sample_pass2 = rdd_apevents_pass2.filter(
                lambda r: ('event_type' in r.keys()) and (r['event_type'] == 'device_events') \
                          and ('ap' in r['source'].keys()) and (r['source']['ap'] in ap_list_positive) and (
                                      r['source']['ev_name'] == "AP_CONFIGURED"))

            outerloopdf_pass2 = rdd_apevents_sample_pass2.map(
                lambda r: (r['source']['ap'], r['source']['ev_name'], r['source']['config'], r['timestamp'])).toDF(
                pull_schema2)

            df_apscope_withconfig = outerloopdf_pass2.withColumn('row_number', F.row_number().over(
                Window.partitionBy('ap').orderBy(F.col('timestamp').desc())))

            step_window = Window.partitionBy('ap').orderBy(F.col("row_number").desc())
            df_apscope_withconfig = df_apscope_withconfig.withColumn('config_step',
                                                                     F.lag('config', offset=1).over(step_window))
            # df_apscope_withconfig.persist()
            # df_apscope_withconfig.show(3, )

            def getconfigdiff_def(x, y):
                if (x == "") or (y == ""):
                    return "'config not found'"
                else:
                    try:
                        j1 = str(x)  # json.loads(x)
                        j2 = str(y)  # json.loads(y)
                        l1 = [x for x in difflib.unified_diff(j1, j2, n=100, lineterm='') if "@@" in x]
                        retlist = list()
                        for x_ in l1:
                            sp = x_.replace("@", "").replace(" ", "").split("+")[1].split(",")
                            s_ = int(sp[0])
                            e_ = int(sp[1])
                            if sp != []:
                                print(s_, e_)
                                retlist.append([{"new": j1[s_: s_ + e_ - 1], "old": j2[s_: s_ + e_ - 1]}])
                        retval = str(retlist)
                        return retval
                    except:
                        retval = "error"
                    return retval

            getconfigdiff = F.udf(getconfigdiff_def, T.StringType())

            df_apscope_withconfigdiff = df_apscope_withconfig.select('ap', 'timestamp', 'row_number',
                                                                     F.to_json(F.col('config')).alias('config'),
                                                                     F.to_json(F.col('config_step')).alias('config_step')) \
                .withColumn('config_change', getconfigdiff(F.col('config'), F.col('config_step')))

            # df_apscope_withconfigdiff.orderBy('ap', 'row_number').show(10, )
            df_m1 = df_select_scope_events_apscope.select('row_key', 'start_time', 'ap').join(
                df_apscope_withconfigdiff, on='ap', how='left')

            # df_m1.filter("ap=='5c5b351f1ee0'").orderBy('ap', 'row_number').show(10, )
            df_select_scope_events_apscope.unpersist()  # code review 1 needs to unpersist
            df_select_scope_events_apscope_p2 = df_m1.withColumn('time_delta',
                                                               F.col('start_time') - F.col('timestamp') / 1000) \
                .filter('time_delta>0') \
                .withColumn('row_number_timedelta', F.row_number().over(
                Window.partitionBy('ap', 'row_key', 'start_time').orderBy(F.col('time_delta').asc()))) \
                .filter("row_number_timedelta=1")
            # Change above filter condition on row_number_timedelta when you want to push more features out
            # df_select_scope_events_apscope_p2.filter("ap=='5c5b351f1ee0'").orderBy('ap', 'row_number_timedelta').show(20, )

            # df_select_scope_events_apscope_p2.persist()  # code review 1 needs to not persist
            # print('\n---\n %d \n---' % df_select_scope_events_apscope_p2.count())
            # outFileName = "s3://mist-staging-jupyterhub-notebooks/jupyter/abhakta@juniper.net/emr-nb/data/scope_with_ap_configs_" \
            #               + str(self.start_epoch) + "_" + str(self.end_epoch)
            # print('\n\n\n Writing to file %s' % outFileName)
            # df_select_scope_events_apscope_p2.filter("row_number_timedelta=1").write.mode('overwrite').parquet(outFileName)
            return_val = df_select_scope_events_apscope_p2
        else:
            return_val = None
        return return_val


def update_event_type(eve_rdd, updated_entry):
    """
    Need to update the entity event type to a new event type to identify as an appended entity event
    """
    eve_rdd['event_type'] = updated_entry
    return eve_rdd

def add_config_meta(x):
    """
    Update the details field of original entity event with ap config change history and other details
    """
    x_val = x[1][1]  #original event info
    x_val_details = x_val['details']
    x_val_details['apconfig_history'] = x[1][0]  #info from config diff code
    x_val['details'] = x_val_details
    return x_val

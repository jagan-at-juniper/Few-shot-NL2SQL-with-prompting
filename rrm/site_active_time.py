import datetime
import time
from pyspark.sql import functions
from pyspark.sql.functions import udf, lit, explode, col, countDistinct
from pyspark.sql.types import StringType
import requests
from example import Example


def api(site_id):
    url = 'http://papi-internal-production.mistsys.net/internal/sites/' + site_id
    r = requests.get(url)
    if r.status_code == 200:
        js = r.json()
        return js['timezone']


def appendZero(val):
    val = str(val)
    if len(val) == 1:
        val = '0' + val
    return val


class S3Read(Example):

    def run_by_day(self, epoch):
        start_day = datetime.datetime.utcfromtimestamp(epoch)
        date = '{yyyy}-{mm}-{dd}'.format(yyyy=start_day.year, mm=start_day.month, dd=appendZero(start_day.day))
        for x in range(0, 24):
            current = start_day + datetime.timedelta(hours=x)
            hh = appendZero(current.hour)
            timestamp = datetime.datetime.timestamp(current)
            s3_path = "s3://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/dt" \
                      "={date}/hr={hour}/*".format(date=date, hour=hh)
            csv_path = 's3://mist-data-science-dev/rrm_scheduler_optimization/ap-stats-analytics-production/{date}/{hr}' \
                .format(date=date, hr=hh)
            start = time.time()
            self.info('reading data from s3 ' + s3_path)
            test = self.spark.read.parquet(s3_path)
            self.info('finished reading data from s3 ' + str(time.time()))
            count = test.count()
            self.info('data frame count %d' % count)
            test.createOrReplaceTempView('test')
            df1 = test.select('id', 'org_id', 'site_id', 'total_client_count', 'when','active_client_count',explode("radios").alias('radios'))
            df1 = df1.selectExpr('id', 'when', 'org_id', 'site_id', 'total_client_count', 'active_client_count','radios.dev as dev','radios.band as band', 'radios.num_active_clients as num_active_clients', 'radios.active_client_wcid as active_client_wcid')
            df2 = df1.selectExpr('id', 'org_id', 'site_id', 'total_client_count', 'active_client_count','dev','band', 'num_active_clients', 'active_client_wcid').filter(col('dev') != 'r2').filter(col('num_active_clients') != 0).withColumn('when1', lit(date)).withColumn('hour', lit(hh)).withColumn('timestamp', lit(str(timestamp))).filter(col('site_id') != '')
            df3 = df2.select('id', 'org_id', 'site_id','timestamp', 'when1', 'hour', 'band', explode('active_client_wcid').alias('active_client_wcid')).withColumn('num_active_clients', lit(1))
            df4 = df3.groupBy('org_id', 'site_id','timestamp', 'when1', 'hour', 'band').agg(countDistinct(col('active_client_wcid')).alias('unique_client'), functions.sum(col('num_active_clients')).alias('active_client_minutes'))
            func = udf(lambda y: api(y), StringType())
            df5 = df4.select('org_id', 'site_id','timestamp', 'when1', 'hour', 'band', 'unique_client', 'active_client_minutes').withColumn('timezone', func(col('site_id')))
            df5.repartition(1).write.csv(csv_path)
            self.info('finished writing data to  s3 dir ' + str(time.time() - start) + ' seconds')
        #df1.select('id', 'org_id', 'site_id', 'total_client_count', 'active_client_count','radios.band', 'radios.num_active_clients', 'radios.active_client_wcid').filter(col('total_client_count') != 0).filter(col('num_active_clients') != 0).dropDuplicates().show(65)
        #self.info(test.select('id', 'site_id', 'org_id', 'total_client_count', 'active_client_count').repartition(1).write.csv("test1.csv"))
        '''df = test.select('id', 'site_id', 'org_id', 'total_client_count', 'active_client_count').withColumn('when1', lit('2019-12-05')).withColumn('hour', lit('00'))
        df = df.groupBy('org_id', 'site_id', 'when1', 'hour').agg(
            {'active_client_count':'sum', 'total_client_count':'sum' }
        )
        
        self.info(df.repartition(1).write.csv('test.csv'))
        self.info(str(time.time() - start) + " seconds")'''


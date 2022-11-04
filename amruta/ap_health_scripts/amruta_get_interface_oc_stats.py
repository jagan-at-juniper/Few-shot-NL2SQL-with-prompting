#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# 128
# T
# device: model = 'SSR'
# only in aws
# staging

# In[1]:


date = '2021-07-28'

# # Staging Data

# In[ ]:


# Load Data

# Data Flattening
import numpy as np
import pandas as pd

import datetime
from datetime import datetime

import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, IntegerType, BooleanType, ArrayType, FloatType, MapType, LongType
from pyspark.sql.functions import lit, udf, size, avg, min as min_, max as max_, sum as sum_, count, countDistinct, \
    arrays_zip, col, mean, stddev, struct, explode, explode_outer, unix_timestamp, sum as sum_
from operator import itemgetter
import json

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

from pyspark.ml.feature import Bucketizer

from pyspark.mllib.util import MLUtils
from pyspark.ml.feature import StandardScaler

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from pyspark.sql import functions as F
from pyspark.sql.functions import collect_list, collect_set, row_number, dense_rank, lead, lag, rank
from pyspark.sql.window import Window

from pyspark.ml.classification import LogisticRegressionModel

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn import metrics
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler

import collections
from collections import deque
import redis
import json


# flatten interface

def to_dfin(df):
    dfin = df.select('id',
                     'mac',
                     'org_id',
                     'site_id',
                     'when',
                     'remote_addr',
                     'hostname',
                     'model',
                     'firmware_version',
                     'serial_number',
                     'memory_used_kb',
                     'memory_total_kb',
                     'slab_num_objs',
                     'slab_obj_size')


    dfin = dfin.filter(col('name').isNotNull())
    vp_udf = udf(lambda x: 'phy' if (x.startswith('ge')) | (x.startswith('xe')) | (x.startswith('et')) | (
        x.startswith('mge')) else 'vir', StringType())
    dfin = dfin.withColumn('type', vp_udf('name'))

    # dfin = dfin.filter(col('media_type')=='copper')

    convert_time = udf(lambda x: datetime.utcfromtimestamp(x / 1000000).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')[:16],
                       StringType())
    dfin = dfin.withColumn('time', convert_time('when'))

    # Filtering phy ports, link up and mac_count>1
    dfin = dfin.filter(col('delta') == True)
    # dfin = dfin.filter(col('link')==True)
    dfin = dfin.filter(dfin.type == 'phy')

    # dfin = dfin.filter(dfin.mac_count>1)

    # add chassis number
    def reix(x):
        try:
            return int(x[x.index('-') + len('-'):x.index('-') + len('-') + 1])
        except e as Exception:
            return -1

    reix_udf = udf(lambda x: reix(x), IntegerType())
    dfin = dfin.withColumn('re_ix', reix_udf(dfin.name))

    # dfin = dfin.filter(col('unconfigured')==False)
    lenudf = udf(lambda x: len(x), IntegerType())
    dfin = dfin.withColumn('len_vlans', lenudf('vlan_ids'))
    # dfin = dfin.filter(dfin.len_vlans>0)

    return dfin


df = spark.read.parquet('s3://mist-secorapp-staging/oc-stats-analytics/oc-stats-analytics-staging/dt=' + date + '/*')
# df = spark.read.parquet('s3://mist-secorapp-production/oc-stats-analytics/oc-stats-analytics-production/dt='+date+'/*')
dfin = to_dfin(df)
dfin.persist().first()

# In[ ]:


uni_udf = udf(lambda x: x[0] - x[1] - x[2], IntegerType())
dfin1 = dfin.withColumn('rx_ucast_packets', uni_udf(struct('rx_packets', 'rx_mcast_packets', 'rx_bcast_packets')))

# In[ ]:


# In[ ]:


# In[ ]:


# In[ ]:


# # Production Data

# In[ ]:


# In[1]:


date = '2022-08-20'
# 4.17


# In[ ]:


import numpy as np
import pandas as pd

import datetime
from datetime import datetime

import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, IntegerType, BooleanType, ArrayType, FloatType, MapType, LongType
from pyspark.sql.functions import lit, udf, size, avg, min as min_, max as max_, sum as sum_, count, countDistinct, \
    arrays_zip, col, mean, stddev, struct, explode, explode_outer, unix_timestamp, sum as sum_
from operator import itemgetter
import json

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

from pyspark.ml.feature import Bucketizer

from pyspark.mllib.util import MLUtils
from pyspark.ml.feature import StandardScaler

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from pyspark.sql import functions as F
from pyspark.sql.functions import collect_list, collect_set, row_number, dense_rank, lead, lag, rank
from pyspark.sql.window import Window

from pyspark.ml.classification import LogisticRegressionModel

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn import metrics
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler

import collections
from collections import deque
import redis
import json


# flatten interface

def to_dfin(df):



    dfin = dfin.filter(col('name').isNotNull())
    vp_udf = udf(lambda x: 'phy' if (x.startswith('ge')) | (x.startswith('xe')) | (x.startswith('et')) | (
        x.startswith('mge')) else 'vir', StringType())
    dfin = dfin.withColumn('type', vp_udf('name'))

    # dfin = dfin.filter(col('media_type')=='copper')

    convert_time = udf(lambda x: datetime.utcfromtimestamp(x / 1000000).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')[:16],
                       StringType())
    dfin = dfin.withColumn('time', convert_time('when'))

    # Filtering phy ports, link up and mac_count>1
    dfin = dfin.filter(col('delta') == True)
    # dfin = dfin.filter(col('link')==True)
    dfin = dfin.filter(dfin.type == 'phy')

    # dfin = dfin.filter(dfin.mac_count>1)

    # add chassis number
    def reix(x):
        try:
            return int(x[x.index('-') + len('-'):x.index('-') + len('-') + 1])
        except Exception as e:
            return -1

    reix_udf = udf(lambda x: reix(x), IntegerType())
    dfin = dfin.withColumn('re_ix', reix_udf(dfin.name))

    # dfin = dfin.filter(col('unconfigured')==False)
    lenudf = udf(lambda x: len(x), IntegerType())
    dfin = dfin.withColumn('len_vlans', lenudf('vlan_ids'))
    # dfin = dfin.filter(dfin.len_vlans>0)

    return dfin


def to_dfin(df):

    dfin = df.select('id',
                     'mac',
                     'org_id',
                     'site_id',
                     'when',
                     'remote_addr',
                     'hostname',
                     'model',
                     'firmware_version',
                     'serial_number',
                     'stpbridge_protocol',
                     'stpbridge_root_id',
                     'stpbridge_root_cost',
                     'stpbridge_root_port',
                     'stpbridge_hello_time',
                     'stpbridge_max_age',
                     'stpbridge_forward_delay',
                     'stpbridge_msg_age',
                     'stpbridge_topo_chg_cnt',
                     'stpbridge_last_topo_chg',
                     'stpbridge_bridge_id',
                     'uptime',
                     'stats_delta',
                     'delta',
                     'deltaErrorMsg',
                     'delta_interval',
                     'poe_controller_module_index',
                     'poe_controller_max_power',
                     'poe_controller_consumption',
                     'poe_controller_guardband',
                     'device_type',
                     explode_outer('interfaces').alias('interfaces'))

    dfin = dfin.select('id',
                       'mac',
                       'org_id',
                       'site_id',
                       'when',
                       'remote_addr',
                       'hostname',
                       'model',
                       'firmware_version',
                       'serial_number',
                       'stpbridge_protocol',
                       'stpbridge_root_id',
                       'stpbridge_root_cost',
                       'stpbridge_root_port',
                       'stpbridge_hello_time',
                       'stpbridge_max_age',
                       'stpbridge_forward_delay',
                       'stpbridge_msg_age',
                       'stpbridge_topo_chg_cnt',
                       'stpbridge_last_topo_chg',
                       'stpbridge_bridge_id',
                       'uptime',
                       'stats_delta',
                       'delta',
                       'deltaErrorMsg',
                       'delta_interval',
                       'poe_controller_module_index',
                       'poe_controller_max_power',
                       'poe_controller_consumption',
                       'poe_controller_guardband',
                       'device_type',
                       col('interfaces.name').alias('name'),
                       col('interfaces.link').alias('link'),
                       col('interfaces.full_duplex').alias('full_duplex'),
                       col('interfaces.mbps').alias('mbps'),
                       col('interfaces.mtu').alias('mtu'),
                       col('interfaces.address').alias('address'),
                       col('interfaces.admin_status').alias('admin_status'),
                       col('interfaces.last_flapped').alias('last_flapped'),
                       col('interfaces.errors').alias('errors'),
                       col('interfaces.poe_enabled').alias('poe_enabled'),
                       col('interfaces.poe_status').alias('poe_status'),
                       col('interfaces.poe_power_limit').alias('poe_power_limit'),
                       col('interfaces.poe_power').alias('poe_power'),
                       col('interfaces.poe_priority').alias('poe_priority'),
                       col('interfaces.poe_class').alias('poe_class'),
                       col('interfaces.poe_mode').alias('poe_mode'),
                       col('interfaces.rx_bytes').alias('rx_bytes'),
                       col('interfaces.tx_bytes').alias('tx_bytes'),
                       col('interfaces.rx_packets').alias('rx_packets'),
                       col('interfaces.tx_packets').alias('tx_packets'),
                       col('interfaces.rx_ucast_packets').alias('rx_ucast_packets'),
                       col('interfaces.tx_ucast_packets').alias('tx_ucast_packets'),
                       col('interfaces.rx_bps').alias('rx_bps'),
                       col('interfaces.tx_bps').alias('tx_bps'),
                       col('interfaces.rx_errors').alias('rx_errors'),
                       col('interfaces.rx_undersize_errors').alias('rx_undersize_errors'),
                       col('interfaces.rx_oversize_errors').alias('rx_oversize_errors'),
                       col('interfaces.rx_fcserrors').alias('rx_fcserrors'),
                       col('interfaces.rx_overrun_errors').alias('rx_overrun_errors'),
                       col('interfaces.rx_discards').alias('rx_discards'),
                       col('interfaces.tx_errors').alias('tx_errors'),
                       col('interfaces.tx_drops').alias('tx_drops'),
                       col('interfaces.tx_mtuerrors').alias('tx_mtuerrors'),
                       col('interfaces.txcarrier_transition').alias('txcarrier_transition'),
                       col('interfaces.tx_mcast_packets').alias('tx_mcast_packets'),
                       col('interfaces.tx_bcast_packets').alias('tx_bcast_packets'),
                       col('interfaces.rx_mcast_packets').alias('rx_mcast_packets'),
                       col('interfaces.rx_bcast_packets').alias('rx_bcast_packets'),
                       col('interfaces.rx_l3_incompletes').alias('rx_l3_incompletes'),
                       col('interfaces.rx_l2_channel_error').alias('rx_l2_channel_error'),
                       col('interfaces.rx_l2_mismatch_timeouts').alias('rx_l2_mismatch_timeouts'),
                       col('interfaces.rx_fifo_errors').alias('rx_fifo_errors'),
                       col('interfaces.rx_resource_errors').alias('rx_resource_errors'),
                       col('interfaces.auto_negotiation_status').alias('auto_negotiation_status'),
                       col('interfaces.mac_count').alias('mac_count'),
                       col('interfaces.unconfigured').alias('unconfigured'),
                       col('interfaces.vlan_ids').alias('vlan_ids'),
                       col('interfaces.media_type').alias('media_type'),
                       col('interfaces.remote_mac').alias('remote_mac'),
                       col('interfaces.remote_hardware').alias('remote_hardware'),
                       col('interfaces.remote_manufacture').alias('remote_manufacture'),
                       col('interfaces.remote_system_desc').alias('remote_system_desc'))

    dfin = dfin.filter(col('name').isNotNull())
    vp_udf = udf(lambda x: 'phy' if (x.startswith('ge')) | (x.startswith('xe')) | (x.startswith('et')) | (
        x.startswith('mge')) else 'vir', StringType())
    dfin = dfin.withColumn('type', vp_udf('name'))

    # dfin = dfin.filter(col('media_type')=='copper')

    convert_time = udf(lambda x: datetime.utcfromtimestamp(x / 1000000).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')[:16],
                       StringType())
    dfin = dfin.withColumn('time', convert_time('when'))

    # Filtering phy ports, link up and mac_count>1
    dfin = dfin.filter(col('delta') == True)
    # dfin = dfin.filter(col('link')==True)
    dfin = dfin.filter(dfin.type == 'phy')

    # dfin = dfin.filter(dfin.mac_count>1)

    # add chassis number
    def reix(x):
        try:
            return int(x[x.index('-') + len('-'):x.index('-') + len('-') + 1])
        except Exception as e:
            return -1

    reix_udf = udf(lambda x: reix(x), IntegerType())
    dfin = dfin.withColumn('re_ix', reix_udf(dfin.name))

    # dfin = dfin.filter(col('unconfigured')==False)
    lenudf = udf(lambda x: len(x), IntegerType())
    dfin = dfin.withColumn('len_vlans', lenudf('vlan_ids'))
    # dfin = dfin.filter(dfin.len_vlans>0)

    return dfin


df = spark.read.parquet(
    's3://mist-secorapp-production/oc-stats-analytics/oc-stats-analytics-production/dt=' + date + '/*')
dfin = to_dfin(df)
dfin.persist().first()

# In[ ]:


# In[ ]:


# In[ ]:


import datetime
from datetime import timedelta, datetime

start_time = datetime.strptime('2022-10-02', '%Y-%m-%d')
end_time = datetime.strptime('2022-10-03', '%Y-%m-%d')

flag = True
while start_time <= end_time:
    date = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')[:10]
    inpath = "s3://mist-secorapp-staging/oc-stats-analytics/oc-stats-analytics-staging/dt=" + date + "/*"
    try:
        df = spark.read.parquet(inpath)
        dfin = to_dfin(df)
        dfin = dfin.filter(col('mac') != '0c8126c7054d').filter(col('auto_negotiation_status') == 'incomplete').filter(
            col('link') == True)
        if dfin.count() > 0:
            print('%s has incomplete negotiation, count of %d' % (date, dfin.count()))
            if flag:
                dffinal = dfin
                flag = False
            else:
                dffinal = dffinal.union(dfin)
        else:
            pass
        start_time = start_time + timedelta(minutes=1440)
    except Exceptions as e:
        print('%s data not exist' % date)

# In[ ]:


dffinal.repartition(1).write.parquet('s3://mist-data-science-dev/jing/model_input_data/negotiation_incomplete')

# In[ ]:


# # Bad cable validation

# In[ ]:


df0 = dfin.filter(col('mac') == mac)
df0.filter(col('name') == port).select('time', 'link', 'poe_status', 'poe_power', 'rx_fcserrors', 'rx_packets',
                                       'tx_packets').sort('time').show(500)

# In[ ]:


# In[ ]:


# In[ ]:


# In[ ]:


# In[ ]:


# In[ ]:


# In[ ]:


# In[ ]:


import numpy as np
import pandas as pd

import datetime
from datetime import datetime

import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, IntegerType, BooleanType, ArrayType, FloatType, MapType, LongType
from pyspark.sql.functions import lit, udf, size, avg, min as min_, max as max_, sum as sum_, count, countDistinct, \
    arrays_zip, col, mean, stddev, struct, explode, explode_outer, unix_timestamp, sum as sum_
from operator import itemgetter
import json

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

from pyspark.ml.feature import Bucketizer

from pyspark.mllib.util import MLUtils
from pyspark.ml.feature import StandardScaler

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from pyspark.sql import functions as F
from pyspark.sql.functions import collect_list, collect_set, row_number, dense_rank, lead, lag, rank
from pyspark.sql.window import Window

from pyspark.ml.classification import LogisticRegressionModel

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn import metrics
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler

import collections
from collections import deque
import redis
import json


# flatten interface

def to_dfin(df):
    dfin = df.select('id',
                     'mac',
                     'org_id',
                     'site_id',
                     'when',
                     'remote_addr',
                     'hostname',
                     'model',
                     'firmware_version',
                     'serial_number',
                     'stpbridge_protocol',
                     'stpbridge_root_id',
                     'stpbridge_root_cost',
                     'stpbridge_root_port',
                     'stpbridge_hello_time',
                     'stpbridge_max_age',
                     'stpbridge_forward_delay',
                     'stpbridge_msg_age',
                     'stpbridge_topo_chg_cnt',
                     'stpbridge_last_topo_chg',
                     'stpbridge_bridge_id',
                     'uptime',
                     'stats_delta',
                     'delta',
                     'deltaErrorMsg',
                     'delta_interval',
                     'poe_controller_module_index',
                     'poe_controller_max_power',
                     'poe_controller_consumption',
                     'poe_controller_guardband',
                     'device_type',
                     'clients',
                     explode_outer('interfaces').alias('interfaces'))

    dfin = dfin.select('id',
                       'mac',
                       'org_id',
                       'site_id',
                       'when',
                       'remote_addr',
                       'hostname',
                       'model',
                       'firmware_version',
                       'serial_number',
                       'stpbridge_protocol',
                       'stpbridge_root_id',
                       'stpbridge_root_cost',
                       'stpbridge_root_port',
                       'stpbridge_hello_time',
                       'stpbridge_max_age',
                       'stpbridge_forward_delay',
                       'stpbridge_msg_age',
                       'stpbridge_topo_chg_cnt',
                       'stpbridge_last_topo_chg',
                       'stpbridge_bridge_id',
                       'uptime',
                       'stats_delta',
                       'delta',
                       'deltaErrorMsg',
                       'delta_interval',
                       'poe_controller_module_index',
                       'poe_controller_max_power',
                       'poe_controller_consumption',
                       'poe_controller_guardband',
                       'device_type',
                       'interfaces',
                       explode_outer('clients').alias('clients'))

    # dfin3 = dfin2.filter(dfin2.clients)

    dfin = dfin.select('id',
                       'mac',
                       'org_id',
                       'site_id',
                       'when',
                       'remote_addr',
                       'hostname',
                       'model',
                       'firmware_version',
                       'serial_number',
                       'stpbridge_protocol',
                       'stpbridge_root_id',
                       'stpbridge_root_cost',
                       'stpbridge_root_port',
                       'stpbridge_hello_time',
                       'stpbridge_max_age',
                       'stpbridge_forward_delay',
                       'stpbridge_msg_age',
                       'stpbridge_topo_chg_cnt',
                       'stpbridge_last_topo_chg',
                       'stpbridge_bridge_id',
                       'uptime',
                       'stats_delta',
                       'delta',
                       'deltaErrorMsg',
                       'delta_interval',
                       'poe_controller_module_index',
                       'poe_controller_max_power',
                       'poe_controller_consumption',
                       'poe_controller_guardband',
                       'device_type',
                       col('clients.mac').alias('client_mac'),
                       col('clients.iface_name').alias('client_port'),
                       col('clients.vlan_id').alias('client_vlanid'),
                       col('clients.hostname').alias('client_hostname'),
                       col('interfaces.name').alias('name'),
                       col('interfaces.link').alias('link'),
                       col('interfaces.full_duplex').alias('full_duplex'),
                       col('interfaces.mbps').alias('mbps'),
                       col('interfaces.mtu').alias('mtu'),
                       col('interfaces.address').alias('address'),
                       col('interfaces.admin_status').alias('admin_status'),
                       col('interfaces.last_flapped').alias('last_flapped'),
                       col('interfaces.errors').alias('errors'),
                       col('interfaces.poe_enabled').alias('poe_enabled'),
                       col('interfaces.poe_status').alias('poe_status'),
                       col('interfaces.poe_power_limit').alias('poe_power_limit'),
                       col('interfaces.poe_power').alias('poe_power'),
                       col('interfaces.poe_priority').alias('poe_priority'),
                       col('interfaces.poe_class').alias('poe_class'),
                       col('interfaces.poe_mode').alias('poe_mode'),
                       col('interfaces.rx_bytes').alias('rx_bytes'),
                       col('interfaces.tx_bytes').alias('tx_bytes'),
                       col('interfaces.rx_packets').alias('rx_packets'),
                       col('interfaces.tx_packets').alias('tx_packets'),
                       col('interfaces.rx_ucast_packets').alias('rx_ucast_packets'),
                       col('interfaces.tx_ucast_packets').alias('tx_ucast_packets'),
                       col('interfaces.rx_bps').alias('rx_bps'),
                       col('interfaces.tx_bps').alias('tx_bps'),
                       col('interfaces.rx_errors').alias('rx_errors'),
                       col('interfaces.rx_undersize_errors').alias('rx_undersize_errors'),
                       col('interfaces.rx_oversize_errors').alias('rx_oversize_errors'),
                       col('interfaces.rx_fcserrors').alias('rx_fcserrors'),
                       col('interfaces.rx_overrun_errors').alias('rx_overrun_errors'),
                       col('interfaces.rx_discards').alias('rx_discards'),
                       col('interfaces.tx_errors').alias('tx_errors'),
                       col('interfaces.tx_drops').alias('tx_drops'),
                       col('interfaces.tx_mtuerrors').alias('tx_mtuerrors'),
                       col('interfaces.txcarrier_transition').alias('txcarrier_transition'),
                       col('interfaces.tx_mcast_packets').alias('tx_mcast_packets'),
                       col('interfaces.tx_bcast_packets').alias('tx_bcast_packets'),
                       col('interfaces.rx_mcast_packets').alias('rx_mcast_packets'),
                       col('interfaces.rx_bcast_packets').alias('rx_bcast_packets'),
                       col('interfaces.rx_l3_incompletes').alias('rx_l3_incompletes'),
                       col('interfaces.rx_l2_channel_error').alias('rx_l2_channel_error'),
                       col('interfaces.rx_l2_mismatch_timeouts').alias('rx_l2_mismatch_timeouts'),
                       col('interfaces.rx_fifo_errors').alias('rx_fifo_errors'),
                       col('interfaces.rx_resource_errors').alias('rx_resource_errors'),
                       col('interfaces.auto_negotiation_status').alias('auto_negotiation_status'),
                       # col('interfaces.mac_count').alias('mac_count'),
                       col('interfaces.unconfigured').alias('unconfigured'),
                       col('interfaces.vlan_ids').alias('vlan_ids'),
                       col('interfaces.media_type').alias('media_type')
                       )

    dfin = dfin.filter(col('name').isNotNull())
    vp_udf = udf(lambda x: 'phy' if (x.startswith('ge')) | (x.startswith('xe')) | (x.startswith('et')) | (
        x.startswith('mge')) else 'vir', StringType())
    dfin = dfin.withColumn('type', vp_udf('name'))

    # dfin = dfin.filter(col('media_type')=='copper')

    convert_time = udf(lambda x: datetime.utcfromtimestamp(x / 1000000).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')[:16],
                       StringType())
    dfin = dfin.withColumn('time', convert_time('when'))

    # Filtering phy ports, link up and mac_count>1
    dfin = dfin.filter(col('delta') == True)
    # dfin = dfin.filter(col('link')==True)
    dfin = dfin.filter(dfin.type == 'phy')

    # dfin = dfin.filter(dfin.mac_count>1)

    # add chassis number
    def reix(x):
        try:
            return int(x[x.index('-') + len('-'):x.index('-') + len('-') + 1])
        except Exception as e:
            return -1

    reix_udf = udf(lambda x: reix(x), IntegerType())
    dfin = dfin.withColumn('re_ix', reix_udf(dfin.name))

    # dfin = dfin.filter(col('unconfigured')==False)
    lenudf = udf(lambda x: len(x), IntegerType())
    dfin = dfin.withColumn('len_vlans', lenudf('vlan_ids'))
    # dfin = dfin.filter(dfin.len_vlans>0)

    return dfin


df = spark.read.parquet(
    's3://mist-secorapp-production/oc-stats-analytics/oc-stats-analytics-production/dt=' + date + '/*')
dfin = to_dfin(df)
dfin.persist().first()

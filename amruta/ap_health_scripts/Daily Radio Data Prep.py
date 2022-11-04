#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Starting from 2.1 adding 
-- mac_stats_rxf0ovfl: integer (nullable = true)
-- mac_stats_rxhlovfl: integer (nullable = true)
# Starting from 2.7 adding
-- rx_decrypt_failures: integer (nullable = true)


# In[1]:


import numpy as np
import pandas as pd
import sklearn

import pyspark

#np.printoptions(precision=2, suppress=True)
np.set_printoptions( suppress=True)

from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

import datetime
from datetime import datetime, timedelta
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, IntegerType, ArrayType, BooleanType, FloatType
import pyspark.sql.functions as fn
from pyspark.sql.functions import lit, collect_list, udf, size, avg, count, countDistinct, col, max as max_, min as min_, sum as sum_, udf, struct, explode
from operator import itemgetter
import json

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

from pyspark.sql.functions import mean, stddev, col 

from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation

from sklearn.datasets import load_iris
from sklearn.linear_model import LogisticRegression

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

import datetime
from datetime import datetime, timedelta

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

from pyspark.ml.feature import Bucketizer


# # New Daily Data

# In[ ]:


"""
#spark = SparkSession.builder.getOorCreate()
def todf(df_in):


    df = df_in.filter(df_in.radios.isNotNull()) \
            .filter(df_in.delta==True) \
            .filter(df_in.wlans[0].isNotNull())

    df = df.select( 'id',
                     'site_id',
                     'org_id',
                   'model',
                   'terminator_timestamp',
                   'config_time',
                   'total_client_count',
                   'active_client_count',
                   'delta',
                   #'wlans',
                   'uptime',
                   'firmware_version', 
                   'pwr_available',
                     explode(df.radios).alias('radios'))\
                .repartition(1000)


    df = df.select('id',
                   'site_id',
                   'org_id',
                   'model',
                   'terminator_timestamp',
                   'config_time',
                   #'total_client_count',
                   #'active_client_count',
                   #'delta',
                   #'wlans',
                   #'nwlans',
                   'uptime',
                   'firmware_version', 
                   'pwr_available',
                   df.radios.rx_mgmt.alias('rx_mgmt'),
                   df.radios.tx_mgmt.alias('tx_mgmt'),
                   df.radios.tx_power.alias('tx_power'),
                   df.radios.bandwidth.alias('bandwidth'),
                     df.radios.max_tx_power.alias('max_tx_power'),
                     df.radios.utilization_all.alias('utilization_all'),
                     df.radios.utilization_tx.alias('utilization_tx'),
                     df.radios.utilization_rx_in_bss.alias('utilization_rx_in_bss'),
                     df.radios.utilization_rx_other_bss.alias('utilization_rx_other_bss'),
                     df.radios.utilization_unknown_wifi.alias('utilization_unknown_wifi'),
                     df.radios.utilization_non_wifi.alias('utilization_non_wifi'),
                     #df.radios.tx_bytes,
                     #df.radios.tx_pkts,
                     #df.radios.tx_mgmt,
                     df.radios.radio_missing.alias('radio_missing'),
                     df.radios.re_init.alias('re_init'),
                     #df.radios.re_init_throttle.alias('re_init_throttle'),
                     df.radios.tx_errors.alias('tx_errors'),
                     #df.radios.rx_bytes,
                     #df.radios.rx_pkts,
                     #df.radios.rx_mgmt,
                     #df.radios.rx_errors,
                   df.radios.tx_pkts_sent_mcast.alias('tx_pkts_sent_mcast'),
                   df.radios.tx_pkts_sent.alias('tx_pkts_sent'),
                     df.radios.tx_failed_arp.alias('tx_failed_arp'),
                     df.radios.tx_drop_arp.alias('tx_drop_arp'),
                     df.radios.tx_psblk_fifo.alias('tx_psblk_fifo'),
                     df.radios.tx_toss.alias('tx_toss'),
                     df.radios.tx_toss_arp.alias('tx_toss_arp'),
                     df.radios.tx_toss_bcmc.alias('tx_toss_bcmc'),
                     df.radios.tx_toss_unicast.alias('tx_toss_unicast'),
                     df.radios.tx_bcmc2unicast.alias('tx_bcmc2unicast'),
                     df.radios.tx_bcmc2unicast_client.alias('tx_bcmc2unicast_client'),
                     df.radios.tx_bcn_intr.alias('tx_bcn_intr'),
                     df.radios.tx_phy_err.alias('tx_phy_err'),
                     df.radios.tx_failed.alias('tx_failed'),
                     df.radios.tx_retries.alias('tx_retries'),
                     df.radios.tx_retried.alias('tx_retried'),
                     df.radios.rx_dups.alias('rx_dups'),
                     df.radios.rx_retried.alias('rx_retried'),
                     df.radios.tx_bcmc2unicast_bytes.alias('tx_bcmc2unicast_bytes'),
                     df.radios.tx_bcmc2unicast_client_bytes.alias('tx_bcmc2unicast_client_bytes'),
                     df.radios.tx_mgmt_dropped.alias('tx_mgmt_dropped'),
                     df.radios.rx_fifo_overflow.alias('rx_fifo_overflow'),
                     df.radios.rx_hl_fifo_overflow.alias('rx_hl_fifo_overflow'),
                     df.radios.rx_hwprobe_req.alias('rx_hwprobe_req'),
                     df.radios.rx_hwprobe_req_qoverflow.alias('rx_hwprobe_req_qoverflow'),
                     df.radios.tx_hwprobe_resp.alias('tx_hwprobe_resp'),
                     df.radios.tx_hwprobe_resp_failed.alias('tx_hwprobe_resp_failed'),
                     df.radios.tx_hwprobe_resp_drop_timeout.alias('tx_hwprobe_resp_drop_timeout'),
                     df.radios.interrupt_stats_tx_bcn_succ.alias('bcn'),
                     df.radios.rx_probe_req_bc.alias('rx_probe_req_bc'),
                     df.radios.rx_probe_req_non_bc.alias('rx_probe_req_non_bc'),
                     df.radios.tx_probe_resp_sw.alias('tx_probe_resp_sw'),
                     df.radios.rx_probe_req_rand.alias('rx_probe_req_rand'),
                     df.radios.rx_bytes.alias('rx_bytes'),
                     df.radios.rx_pkts.alias('rx_pkts'),
                     df.radios.tx_bytes.alias('tx_bytes'),
                     df.radios.tx_pkts.alias('tx_pkts'),
                     df.radios.rx_errors.alias('rx_errors'),
                   df.radios.dev.alias('dev'),
                   df.radios.band.alias('band'),
                   df.radios.channel.alias('channel'),
                   df.radios.secondary_channel.alias('secondary_channel'), #may not in old data
                   df.radios.mac_stats_rxf0ovfl.alias('mac_stats_rxf0ovfl'), #may not in old data
                   df.radios.mac_stats_rxhlovfl.alias('mac_stats_rxhlovfl'), #may not in old data
                   df.radios.rx_decrypt_failures.alias('rx_decrypt_failures'), #may not in old data
                   df.radios.per_antenna_rssi.alias('per_antenna_rssi'), #may not in old data
                   df.radios.num_active_clients.alias('num_active_clients'),
                  df.radios.wlans.alias('wlans'))
                   
    
    #convert time
    convert_time = udf(lambda x: datetime.utcfromtimestamp(x/1000000).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')[:16], StringType())
    concat_udf = F.udf(lambda cols: "_".join([x if x is not None else "*" for x in cols]), StringType())
    nwlans_udf = udf(lambda x: len(x), IntegerType())
    
    df = df.withColumn("id_dev", concat_udf(F.array("id", "band")))\
            .withColumn('nwlans', nwlans_udf('wlans'))\
            .withColumn('time', convert_time('terminator_timestamp'))
    
    df = df.filter(df.nwlans!=0)\
            .filter(df.dev!='r2')

    return df 
"""


# In[ ]:


#spark = SparkSession.builder.getOrCreate()
def todf(df_in):


    df = df_in.filter(df_in.radios.isNotNull())             .filter(df_in.delta==True)             .filter(df_in.wlans[0].isNotNull())

    df = df.select( 'id',
                     'site_id',
                     'org_id',
                   'model',
                   'terminator_timestamp',
                   'config_time',
                   'total_client_count',
                   'active_client_count',
                   'delta',
                   #'wlans',
                   'uptime',
                   'firmware_version', 
                   'pwr_available',
                     explode(df.radios).alias('radios'))\
                .repartition(1000)


    df = df.select('id',
                   'site_id',
                   'org_id',
                   'model',
                   'terminator_timestamp',
                   'config_time',
                   #'total_client_count',
                   #'active_client_count',
                   #'delta',
                   #'wlans',
                   #'nwlans',
                   'uptime',
                   'firmware_version', 
                   'pwr_available',
                df.radios.dev.alias('dev'), df.radios.band.alias('band'), df.radios.channel.alias('channel'), df.radios.secondary_channel.alias('secondary_channel'), df.radios.bandwidth.alias('bandwidth'), df.radios.noise_floor.alias('noise_floor'), df.radios.tx_power.alias('tx_power'), df.radios.max_tx_power.alias('max_tx_power'), df.radios.per_antenna_rssi.alias('per_antenna_rssi'), df.radios.utilization_all.alias('utilization_all'), df.radios.utilization_tx.alias('utilization_tx'), df.radios.utilization_rx_in_bss.alias('utilization_rx_in_bss'), df.radios.utilization_rx_other_bss.alias('utilization_rx_other_bss'), df.radios.utilization_unknown_wifi.alias('utilization_unknown_wifi'), df.radios.utilization_non_wifi.alias('utilization_non_wifi'), df.radios.dfs_radar_detected.alias('dfs_radar_detected'), df.radios.dfs_cac_state.alias('dfs_cac_state'), df.radios.tx_bytes.alias('tx_bytes'), df.radios.tx_pkts.alias('tx_pkts'), df.radios.tx_mgmt.alias('tx_mgmt'), df.radios.tx_errors.alias('tx_errors'), df.radios.rx_bytes.alias('rx_bytes'), df.radios.rx_pkts.alias('rx_pkts'), df.radios.rx_mgmt.alias('rx_mgmt'), df.radios.rx_errors.alias('rx_errors'), df.radios.tx_failed_arp.alias('tx_failed_arp'), df.radios.tx_drop_arp.alias('tx_drop_arp'), df.radios.tx_psblk_fifo.alias('tx_psblk_fifo'), df.radios.tx_toss.alias('tx_toss'), df.radios.tx_toss_arp.alias('tx_toss_arp'), df.radios.tx_toss_bcmc.alias('tx_toss_bcmc'), df.radios.tx_toss_unicast.alias('tx_toss_unicast'), df.radios.tx_bcmc2unicast.alias('tx_bcmc2unicast'), df.radios.tx_bcmc2unicast_client.alias('tx_bcmc2unicast_client'), df.radios.tx_bcn_intr.alias('tx_bcn_intr'), df.radios.tx_phy_err.alias('tx_phy_err'), df.radios.tx_failed.alias('tx_failed'), df.radios.tx_retries.alias('tx_retries'), df.radios.tx_retried.alias('tx_retried'), df.radios.rx_dups.alias('rx_dups'), df.radios.rx_retried.alias('rx_retried'), df.radios.tx_bcmc2unicast_bytes.alias('tx_bcmc2unicast_bytes'), df.radios.tx_bcmc2unicast_client_bytes.alias('tx_bcmc2unicast_client_bytes'), df.radios.re_init.alias('re_init'), df.radios.re_init_throttle.alias('re_init_throttle'), df.radios.tx_mgmt_dropped.alias('tx_mgmt_dropped'), df.radios.rx_fifo_overflow.alias('rx_fifo_overflow'), df.radios.rx_hl_fifo_overflow.alias('rx_hl_fifo_overflow'), df.radios.rx_hwprobe_req.alias('rx_hwprobe_req'), df.radios.rx_hwprobe_req_qoverflow.alias('rx_hwprobe_req_qoverflow'), df.radios.tx_hwprobe_resp.alias('tx_hwprobe_resp'), df.radios.tx_hwprobe_resp_failed.alias('tx_hwprobe_resp_failed'), df.radios.tx_hwprobe_resp_drop_timeout.alias('tx_hwprobe_resp_drop_timeout'), df.radios.ampdutx_fifo_full.alias('ampdutx_fifo_full'), df.radios.ampdutx_drop.alias('ampdutx_drop'), df.radios.ampdutx_stuck.alias('ampdutx_stuck'), df.radios.ampdutx_orphan.alias('ampdutx_orphan'), df.radios.ampdutx_stuck_ps.alias('ampdutx_stuck_ps'), df.radios.ampdutx_r0hole.alias('ampdutx_r0hole'), df.radios.ampdutx_rnhole.alias('ampdutx_rnhole'), df.radios.ampdutx_rlag.alias('ampdutx_rlag'), df.radios.ampdutx_tx_add_ba_req.alias('ampdutx_tx_add_ba_req'), df.radios.ampdutx_rx_add_ba_resp.alias('ampdutx_rx_add_ba_resp'), df.radios.ampdutx_lost.alias('ampdutx_lost'), df.radios.ampdutx_tx_bar.alias('ampdutx_tx_bar'), df.radios.ampdutx_rx_ba.alias('ampdutx_rx_ba'), df.radios.ampdutx_no_ba.alias('ampdutx_no_ba'), df.radios.ampdutx_rx_unexpect.alias('ampdutx_rx_unexpect'), df.radios.ampdutx_tx_del_ba.alias('ampdutx_tx_del_ba'), df.radios.ampdutx_rx_del_ba.alias('ampdutx_rx_del_ba'), df.radios.ampdutx_block_data_fifo.alias('ampdutx_block_data_fifo'), df.radios.ampdutx_orphan_bad_ini.alias('ampdutx_orphan_bad_ini'), df.radios.ampdutx_drop_no_buf.alias('ampdutx_drop_no_buf'), df.radios.ampdutx_drop_err.alias('ampdutx_drop_err'), df.radios.ampdutx_orphan_bad_ini_free.alias('ampdutx_orphan_bad_ini_free'), df.radios.ampdurx_holes.alias('ampdurx_holes'), df.radios.ampdurx_stuck.alias('ampdurx_stuck'), df.radios.ampdurx_rx_add_ba_req.alias('ampdurx_rx_add_ba_req'), df.radios.ampdurx_tx_add_ba_resp.alias('ampdurx_tx_add_ba_resp'), df.radios.ampdurx_rx_bar.alias('ampdurx_rx_bar'), df.radios.ampdurx_tx_ba.alias('ampdurx_tx_ba'), df.radios.ampdurx_rx_unexpect.alias('ampdurx_rx_unexpect'), df.radios.ampdurx_tx_del_ba.alias('ampdurx_tx_del_ba'), df.radios.ampdurx_rx_del_ba.alias('ampdurx_rx_del_ba'), df.radios.num_clients.alias('num_clients'), df.radios.radio_missing.alias('radio_missing'), df.radios.interrupt_stats_tx_bcn_succ.alias('interrupt_stats_tx_bcn_succ'), df.radios.interrupt_stats_dmaint.alias('interrupt_stats_dmaint'), df.radios.narstats_queued.alias('narstats_queued'), df.radios.narstats_dequeued.alias('narstats_dequeued'), df.radios.narstats_held.alias('narstats_held'), df.radios.narstats_dropped.alias('narstats_dropped'), df.radios.phy_type.alias('phy_type'), df.radios.rx_probe_req_bc.alias('rx_probe_req_bc'), df.radios.rx_probe_req_non_bc.alias('rx_probe_req_non_bc'), df.radios.ignore_probe_req_min_rssi.alias('ignore_probe_req_min_rssi'), df.radios.tx_probe_resp_sw.alias('tx_probe_resp_sw'), df.radios.rx_probe_req_rand.alias('rx_probe_req_rand'), df.radios.pkt_queue_requested.alias('pkt_queue_requested'), df.radios.pkt_queue_full_dropped.alias('pkt_queue_full_dropped'), df.radios.pkt_queue_dropped.alias('pkt_queue_dropped'), df.radios.pkt_queue_retried.alias('pkt_queue_retried'), df.radios.pkt_pend_bk.alias('pkt_pend_bk'), df.radios.pkt_pend_be.alias('pkt_pend_be'), df.radios.pkt_pend_vi.alias('pkt_pend_vi'), df.radios.pkt_pend_vo.alias('pkt_pend_vo'), df.radios.pkt_pend_bcmc.alias('pkt_pend_bcmc'), df.radios.pkt_pend_atim.alias('pkt_pend_atim'), df.radios.pkt_pend_common.alias('pkt_pend_common'), df.radios.pkt_pend_ampdu.alias('pkt_pend_ampdu'), df.radios.pkt_pend_amsdu.alias('pkt_pend_amsdu'), df.radios.pkt_pend_ps.alias('pkt_pend_ps'), df.radios.pkt_pend_hw.alias('pkt_pend_hw'), df.radios.pkt_pend_all.alias('pkt_pend_all'), df.radios.wlans.alias('wlans'), df.radios.scan_channel.alias('scan_channel'), df.radios.ampdutx_tx_ampdu_succ.alias('ampdutx_tx_ampdu_succ'), df.radios.ampdurx_rx_mpdu.alias('ampdurx_rx_mpdu'), df.radios.ampdurx_rx_ampdu.alias('ampdurx_rx_ampdu'), df.radios.radio_counter_delta.alias('radio_counter_delta'), df.radios.num_active_clients.alias('num_active_clients'), df.radios.active_client_wcid.alias('active_client_wcid'), df.radios.mac_stats_rxf0ovfl.alias('mac_stats_rxf0ovfl'), df.radios.mac_stats_rxhlovfl.alias('mac_stats_rxhlovfl'), df.radios.rx_decrypt_failures.alias('rx_decrypt_failures'), df.radios.mac_stats_tx_all_frm.alias('mac_stats_tx_all_frm'), df.radios.mac_stats_tx_rts_frm.alias('mac_stats_tx_rts_frm'), df.radios.mac_stats_tx_cts_frm.alias('mac_stats_tx_cts_frm'), df.radios.mac_stats_tx_data_null_frm.alias('mac_stats_tx_data_null_frm'), df.radios.mac_stats_tx_bcn_frm.alias('mac_stats_tx_bcn_frm'), df.radios.mac_stats_tx_ampdu.alias('mac_stats_tx_ampdu'), df.radios.mac_stats_tx_mpdu.alias('mac_stats_tx_mpdu'), df.radios.mac_stats_tx_unfl.alias('mac_stats_tx_unfl'), df.radios.mac_stats_tx_bcn_unfl.alias('mac_stats_tx_bcn_unfl'), df.radios.mac_stats_tx_phy_err.alias('mac_stats_tx_phy_err'), df.radios.mac_stats_rx_ucast_pky_eng.alias('mac_stats_rx_ucast_pky_eng'), df.radios.mac_stats_rx_mcast_pkt_eng.alias('mac_stats_rx_mcast_pkt_eng'), df.radios.mac_stats_rx_frm_too_long.alias('mac_stats_rx_frm_too_long'), df.radios.mac_stats_rx_frm_too_short.alias('mac_stats_rx_frm_too_short'), df.radios.mac_stats_rx_ant_err.alias('mac_stats_rx_ant_err'), df.radios.mac_stats_rx_bad_fas.alias('mac_stats_rx_bad_fas'), df.radios.mac_stats_rx_bad_plcp.alias('mac_stats_rx_bad_plcp'), df.radios.mac_stats_rx_glitch.alias('mac_stats_rx_glitch'), df.radios.mac_stats_rx_start.alias('mac_stats_rx_start'), df.radios.mac_stats_rx_ucast_mbss.alias('mac_stats_rx_ucast_mbss'), df.radios.mac_stats_rx_mgmt_mbss.alias('mac_stats_rx_mgmt_mbss'), df.radios.mac_stats_rx_ctl.alias('mac_stats_rx_ctl'), df.radios.mac_stats_rx_rts.alias('mac_stats_rx_rts'), df.radios.mac_stats_rx_cts.alias('mac_stats_rx_cts'), df.radios.mac_stats_rx_ack.alias('mac_stats_rx_ack'), df.radios.mac_stats_rx_ucast_na.alias('mac_stats_rx_ucast_na'), df.radios.mac_stats_rx_mgmt_na.alias('mac_stats_rx_mgmt_na'), df.radios.mac_stats_rx_ctl_na.alias('mac_stats_rx_ctl_na'), df.radios.mac_stats_rx_rts_na.alias('mac_stats_rx_rts_na'), df.radios.mac_stats_rx_cts_na.alias('mac_stats_rx_cts_na'), df.radios.mac_stats_rx_data_mcast.alias('mac_stats_rx_data_mcast'), df.radios.mac_stats_rx_mgmt_mcast.alias('mac_stats_rx_mgmt_mcast'), df.radios.mac_stats_rx_ctl_mcast.alias('mac_stats_rx_ctl_mcast'), df.radios.mac_stats_rx_bcn_mbss.alias('mac_stats_rx_bcn_mbss'), df.radios.mac_stats_rx_ucast_obss.alias('mac_stats_rx_ucast_obss'), df.radios.mac_stats_rx_bcn_obss.alias('mac_stats_rx_bcn_obss'), df.radios.mac_stats_rx_tsp_timeout.alias('mac_stats_rx_tsp_timeout'), df.radios.mac_stats_tx_bcn_cancel.alias('mac_stats_tx_bcn_cancel'), df.radios.mac_stats_rx_no_delimiter.alias('mac_stats_rx_no_delimiter'), df.radios.mac_stats_rxf1ovfl.alias('mac_stats_rxf1ovfl'), df.radios.mac_stats_miss_bcn_dbg.alias('mac_stats_miss_bcn_dbg'), df.radios.mac_stats_pmqovfl.alias('mac_stats_pmqovfl'), df.radios.mac_stats_rx_hwprobe_req.alias('mac_stats_rx_hwprobe_req'), df.radios.mac_stats_rx_hwprobe_req_qoverflow.alias('mac_stats_rx_hwprobe_req_qoverflow'), df.radios.mac_stats_tx_hwprobe_resp.alias('mac_stats_tx_hwprobe_resp'), df.radios.mac_stats_tx_hwprobe_resp_failed.alias('mac_stats_tx_hwprobe_resp_failed'), df.radios.mac_stats_tx_hwprobe_resp_drop_timeout.alias('mac_stats_tx_hwprobe_resp_drop_timeout'), df.radios.mac_stats_tx_rts_fail.alias('mac_stats_tx_rts_fail'), df.radios.mac_stats_tx_ucast.alias('mac_stats_tx_ucast'), df.radios.mac_stats_tx_in_rts_txop.alias('mac_stats_tx_in_rts_txop'), df.radios.mac_stats_rx_block_ack.alias('mac_stats_rx_block_ack'), df.radios.mac_stats_tx_block_ack.alias('mac_stats_tx_block_ack'), df.radios.mac_stats_phy_rx_glitch.alias('mac_stats_phy_rx_glitch'), df.radios.mac_stats_rx_drop2nd.alias('mac_stats_rx_drop2nd'), df.radios.mac_stats_rx_too_late.alias('mac_stats_rx_too_late'), df.radios.mac_stats_phy_bad_plcp.alias('mac_stats_phy_bad_plcp'), df.radios.tx_pkts_sent.alias('tx_pkts_sent'), df.radios.tx_pkts_sent_mcast.alias('tx_pkts_sent_mcast')
                  )
                   
    
    #convert time
    convert_time = udf(lambda x: datetime.utcfromtimestamp(x/1000000).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')[:16], StringType())
    concat_udf = F.udf(lambda cols: "_".join([x if x is not None else "*" for x in cols]), StringType())
    nwlans_udf = udf(lambda x: len(x), IntegerType())
    
    df = df.withColumn("id_dev", concat_udf(F.array("id", "band")))            .withColumn('nwlans', nwlans_udf('wlans'))            .withColumn('time', convert_time('terminator_timestamp'))
    
    df = df.filter(df.nwlans!=0)            .filter(df.dev!='r2')

    return df 


# In[ ]:





# In[ ]:



date1 = '2022-02-04'
date2 = '2022-02-04'

#from 2019-8-14, data filtered by wlans under radio
start_time = datetime.strptime(date1+'T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
end_time = datetime.strptime(date2+'T23:59:59Z', '%Y-%m-%dT%H:%M:%SZ')


while start_time <= end_time:
    date = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')[:10]
    inpath = "s3://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/dt="+date+"/*" 
    print (inpath)
    try: 
        dfin = spark.read.parquet(inpath).repartition(1000)
        df = todf(dfin)
        #df = df.drop('per_antenna_rssi','wlans','total_client_count','active_client_count','re_init_throttle')
        #df.persist().count()
        #df.repartition(100).write.parquet('s3://mist-data-science-dev/jing/radio_subset2/'+date)
        df.repartition(500).write.parquet('s3://mist-data-science-dev/jing/radio_subset2/'+date)
        print ('saved data')
        #df.unpersist()
    except Exception as e:
        print (inpath, ' schema changed or not exist')
    start_time = start_time + timedelta(minutes=1440)


# In[ ]:


# one ap only

apid = 
date1 = '2021-07-15'
date2 = '2020-07-15'

#from 2019-8-14, data filtered by wlans under radio
start_time = datetime.strptime(date1+'T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
end_time = datetime.strptime(date2+'T23:59:59Z', '%Y-%m-%dT%H:%M:%SZ')


while start_time <= end_time:
    date = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')[:10]
    inpath = "s3a://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/dt="+date+"/*" 
    print (inpath)
    try: 
        dfin = spark.read.parquet(inpath).repartition(1000)
        df = dfin.filter(dfin.id==apid)
        df = todf(df).persist()
        #df = df.drop('per_antenna_rssi','wlans','total_client_count','active_client_count','re_init_throttle')
        #df.persist().count()
        #df.repartition(100).write.parquet('s3://mist-data-science-dev/jing/radio_subset2/'+date)
        #df.repartition(500).write.parquet('s3://mist-data-science-dev/jing/radio_subset2/'+date)
        #df.unpersist()
    except Exception as e:
        print (inpath, ' schema changed or not exist')
    start_time = start_time + timedelta(minutes=1440)

df.toPandas().to_csv(apid+'.csv')


# # with all radios columns

# In[ ]:


#spark = SparkSession.builder.getOrCreate()
def todf(df_in):


    df = df_in.filter(df_in.radios.isNotNull())             .filter(df_in.delta==True)             .filter(df_in.wlans[0].isNotNull())

    df = df.select( 'id',
                     'site_id',
                     'org_id',
                   'model',
                   'terminator_timestamp',
                   'config_time',
                   'total_client_count',
                   'active_client_count',
                   'delta',
                   'uptime',
                   'firmware_version', 
                   'pwr_available',
                     explode(df.radios).alias('radios'))\
                .repartition(200)

    
    df = df.select('id',
                   'site_id',
                   'org_id',
                   'model',
                   'terminator_timestamp',
                   'config_time',
                   'uptime',
                   'firmware_version', 
                   'pwr_available',
 df.radios.dev.alias('dev'),
 df.radios.band.alias('band'),
 df.radios.channel.alias('channel'),
 df.radios.secondary_channel.alias('secondary_channel'),
 df.radios.bandwidth.alias('bandwidth'),
 df.radios.noise_floor.alias('noise_floor'),
 df.radios.tx_power.alias('tx_power'),
 df.radios.max_tx_power.alias('max_tx_power'),
 df.radios.per_antenna_rssi.alias('per_antenna_rssi'),
 df.radios.utilization_all.alias('utilization_all'),
 df.radios.utilization_tx.alias('utilization_tx'),
 df.radios.utilization_rx_in_bss.alias('utilization_rx_in_bss'),
 df.radios.utilization_rx_other_bss.alias('utilization_rx_other_bss'),
 df.radios.utilization_unknown_wifi.alias('utilization_unknown_wifi'),
 df.radios.utilization_non_wifi.alias('utilization_non_wifi'),
 df.radios.dfs_radar_detected.alias('dfs_radar_detected'),
 df.radios.dfs_cac_state.alias('dfs_cac_state'),
 df.radios.tx_bytes.alias('tx_bytes'),
 df.radios.tx_pkts.alias('tx_pkts'),
 df.radios.tx_mgmt.alias('tx_mgmt'),
 df.radios.tx_errors.alias('tx_errors'),
 df.radios.rx_bytes.alias('rx_bytes'),
 df.radios.rx_pkts.alias('rx_pkts'),
 df.radios.rx_mgmt.alias('rx_mgmt'),
 df.radios.rx_errors.alias('rx_errors'),
 df.radios.tx_failed_arp.alias('tx_failed_arp'),
 df.radios.tx_drop_arp.alias('tx_drop_arp'),
 df.radios.tx_psblk_fifo.alias('tx_psblk_fifo'),
 df.radios.tx_toss.alias('tx_toss'),
 df.radios.tx_toss_arp.alias('tx_toss_arp'),
 df.radios.tx_toss_bcmc.alias('tx_toss_bcmc'),
 df.radios.tx_toss_unicast.alias('tx_toss_unicast'),
 df.radios.tx_bcmc2unicast.alias('tx_bcmc2unicast'),
 df.radios.tx_bcmc2unicast_client.alias('tx_bcmc2unicast_client'),
 df.radios.tx_bcn_intr.alias('tx_bcn_intr'),
 df.radios.tx_phy_err.alias('tx_phy_err'),
 df.radios.tx_failed.alias('tx_failed'),
 df.radios.tx_retries.alias('tx_retries'),
 df.radios.tx_retried.alias('tx_retried'),
 df.radios.rx_dups.alias('rx_dups'),
 df.radios.rx_retried.alias('rx_retried'),
 df.radios.tx_bcmc2unicast_bytes.alias('tx_bcmc2unicast_bytes'),
 df.radios.tx_bcmc2unicast_client_bytes.alias('tx_bcmc2unicast_client_bytes'),
 df.radios.re_init.alias('re_init'),
 df.radios.re_init_throttle.alias('re_init_throttle'),
 df.radios.tx_mgmt_dropped.alias('tx_mgmt_dropped'),
 df.radios.rx_fifo_overflow.alias('rx_fifo_overflow'),
 df.radios.rx_hl_fifo_overflow.alias('rx_hl_fifo_overflow'),
 df.radios.rx_hwprobe_req.alias('rx_hwprobe_req'),
 df.radios.rx_hwprobe_req_qoverflow.alias('rx_hwprobe_req_qoverflow'),
 df.radios.tx_hwprobe_resp.alias('tx_hwprobe_resp'),
 df.radios.tx_hwprobe_resp_failed.alias('tx_hwprobe_resp_failed'),
 df.radios.tx_hwprobe_resp_drop_timeout.alias('tx_hwprobe_resp_drop_timeout'),
 df.radios.ampdutx_fifo_full.alias('ampdutx_fifo_full'),
 df.radios.ampdutx_drop.alias('ampdutx_drop'),
 df.radios.ampdutx_stuck.alias('ampdutx_stuck'),
 df.radios.ampdutx_orphan.alias('ampdutx_orphan'),
 df.radios.ampdutx_stuck_ps.alias('ampdutx_stuck_ps'),
 df.radios.ampdutx_r0hole.alias('ampdutx_r0hole'),
 df.radios.ampdutx_rnhole.alias('ampdutx_rnhole'),
 df.radios.ampdutx_rlag.alias('ampdutx_rlag'),
 df.radios.ampdutx_tx_add_ba_req.alias('ampdutx_tx_add_ba_req'),
 df.radios.ampdutx_rx_add_ba_resp.alias('ampdutx_rx_add_ba_resp'),
 df.radios.ampdutx_lost.alias('ampdutx_lost'),
 df.radios.ampdutx_tx_bar.alias('ampdutx_tx_bar'),
 df.radios.ampdutx_rx_ba.alias('ampdutx_rx_ba'),
 df.radios.ampdutx_no_ba.alias('ampdutx_no_ba'),
 df.radios.ampdutx_rx_unexpect.alias('ampdutx_rx_unexpect'),
 df.radios.ampdutx_tx_del_ba.alias('ampdutx_tx_del_ba'),
 df.radios.ampdutx_rx_del_ba.alias('ampdutx_rx_del_ba'),
 df.radios.ampdutx_block_data_fifo.alias('ampdutx_block_data_fifo'),
 df.radios.ampdutx_orphan_bad_ini.alias('ampdutx_orphan_bad_ini'),
 df.radios.ampdutx_drop_no_buf.alias('ampdutx_drop_no_buf'),
 df.radios.ampdutx_drop_err.alias('ampdutx_drop_err'),
 df.radios.ampdutx_orphan_bad_ini_free.alias('ampdutx_orphan_bad_ini_free'),
 df.radios.ampdurx_holes.alias('ampdurx_holes'),
 df.radios.ampdurx_stuck.alias('ampdurx_stuck'),
 df.radios.ampdurx_rx_add_ba_req.alias('ampdurx_rx_add_ba_req'),
 df.radios.ampdurx_tx_add_ba_resp.alias('ampdurx_tx_add_ba_resp'),
 df.radios.ampdurx_rx_bar.alias('ampdurx_rx_bar'),
 df.radios.ampdurx_tx_ba.alias('ampdurx_tx_ba'),
 df.radios.ampdurx_rx_unexpect.alias('ampdurx_rx_unexpect'),
 df.radios.ampdurx_tx_del_ba.alias('ampdurx_tx_del_ba'),
 df.radios.ampdurx_rx_del_ba.alias('ampdurx_rx_del_ba'),
 df.radios.num_clients.alias('num_clients'),
 df.radios.radio_missing.alias('radio_missing'),
 df.radios.interrupt_stats_tx_bcn_succ.alias('interrupt_stats_tx_bcn_succ'),
 df.radios.interrupt_stats_dmaint.alias('interrupt_stats_dmaint'),
 df.radios.narstats_queued.alias('narstats_queued'),
 df.radios.narstats_dequeued.alias('narstats_dequeued'),
 df.radios.narstats_held.alias('narstats_held'),
 df.radios.narstats_dropped.alias('narstats_dropped'),
 df.radios.phy_type.alias('phy_type'),
 df.radios.rx_probe_req_bc.alias('rx_probe_req_bc'),
 df.radios.rx_probe_req_non_bc.alias('rx_probe_req_non_bc'),
 df.radios.ignore_probe_req_min_rssi.alias('ignore_probe_req_min_rssi'),
 df.radios.tx_probe_resp_sw.alias('tx_probe_resp_sw'),
 df.radios.rx_probe_req_rand.alias('rx_probe_req_rand'),
 df.radios.pkt_queue_requested.alias('pkt_queue_requested'),
 df.radios.pkt_queue_full_dropped.alias('pkt_queue_full_dropped'),
 df.radios.pkt_queue_dropped.alias('pkt_queue_dropped'),
 df.radios.pkt_queue_retried.alias('pkt_queue_retried'),
 df.radios.pkt_pend_bk.alias('pkt_pend_bk'),
 df.radios.pkt_pend_be.alias('pkt_pend_be'),
 df.radios.pkt_pend_vi.alias('pkt_pend_vi'),
 df.radios.pkt_pend_vo.alias('pkt_pend_vo'),
 df.radios.pkt_pend_bcmc.alias('pkt_pend_bcmc'),
 df.radios.pkt_pend_atim.alias('pkt_pend_atim'),
 df.radios.pkt_pend_common.alias('pkt_pend_common'),
 df.radios.pkt_pend_ampdu.alias('pkt_pend_ampdu'),
 df.radios.pkt_pend_amsdu.alias('pkt_pend_amsdu'),
 df.radios.pkt_pend_ps.alias('pkt_pend_ps'),
 df.radios.pkt_pend_hw.alias('pkt_pend_hw'),
 df.radios.pkt_pend_all.alias('pkt_pend_all'),
 df.radios.wlans.alias('wlans'),
 df.radios.scan_channel.alias('scan_channel'),
 df.radios.ampdutx_tx_ampdu_succ.alias('ampdutx_tx_ampdu_succ'),
 df.radios.ampdurx_rx_mpdu.alias('ampdurx_rx_mpdu'),
 df.radios.ampdurx_rx_ampdu.alias('ampdurx_rx_ampdu'),
 df.radios.radio_counter_delta.alias('radio_counter_delta'),
 df.radios.num_active_clients.alias('num_active_clients'),
 df.radios.active_client_wcid.alias('active_client_wcid'),
 df.radios.mac_stats_rxf0ovfl.alias('mac_stats_rxf0ovfl'),
 df.radios.mac_stats_rxhlovfl.alias('mac_stats_rxhlovfl'),
 df.radios.rx_decrypt_failures.alias('rx_decrypt_failures'),
 df.radios.mac_stats_tx_all_frm.alias('mac_stats_tx_all_frm'),
 df.radios.mac_stats_tx_rts_frm.alias('mac_stats_tx_rts_frm'),
 df.radios.mac_stats_tx_cts_frm.alias('mac_stats_tx_cts_frm'),
 df.radios.mac_stats_tx_data_null_frm.alias('mac_stats_tx_data_null_frm'),
 df.radios.mac_stats_tx_bcn_frm.alias('mac_stats_tx_bcn_frm'),
 df.radios.mac_stats_tx_ampdu.alias('mac_stats_tx_ampdu'),
 df.radios.mac_stats_tx_mpdu.alias('mac_stats_tx_mpdu'),
 df.radios.mac_stats_tx_unfl.alias('mac_stats_tx_unfl'),
 df.radios.mac_stats_tx_bcn_unfl.alias('mac_stats_tx_bcn_unfl'),
 df.radios.mac_stats_tx_phy_err.alias('mac_stats_tx_phy_err'),
 df.radios.mac_stats_rx_ucast_pky_eng.alias('mac_stats_rx_ucast_pky_eng'),
 df.radios.mac_stats_rx_mcast_pkt_eng.alias('mac_stats_rx_mcast_pkt_eng'),
 df.radios.mac_stats_rx_frm_too_long.alias('mac_stats_rx_frm_too_long'),
 df.radios.mac_stats_rx_frm_too_short.alias('mac_stats_rx_frm_too_short'),
 df.radios.mac_stats_rx_ant_err.alias('mac_stats_rx_ant_err'),
 df.radios.mac_stats_rx_bad_fas.alias('mac_stats_rx_bad_fas'),
 df.radios.mac_stats_rx_bad_plcp.alias('mac_stats_rx_bad_plcp'),
 df.radios.mac_stats_rx_glitch.alias('mac_stats_rx_glitch'),
 df.radios.mac_stats_rx_start.alias('mac_stats_rx_start'),
 df.radios.mac_stats_rx_ucast_mbss.alias('mac_stats_rx_ucast_mbss'),
 df.radios.mac_stats_rx_mgmt_mbss.alias('mac_stats_rx_mgmt_mbss'),
 df.radios.mac_stats_rx_ctl.alias('mac_stats_rx_ctl'),
 df.radios.mac_stats_rx_rts.alias('mac_stats_rx_rts'),
 df.radios.mac_stats_rx_cts.alias('mac_stats_rx_cts'),
 df.radios.mac_stats_rx_ack.alias('mac_stats_rx_ack'),
 df.radios.mac_stats_rx_ucast_na.alias('mac_stats_rx_ucast_na'),
 df.radios.mac_stats_rx_mgmt_na.alias('mac_stats_rx_mgmt_na'),
 df.radios.mac_stats_rx_ctl_na.alias('mac_stats_rx_ctl_na'),
 df.radios.mac_stats_rx_rts_na.alias('mac_stats_rx_rts_na'),
 df.radios.mac_stats_rx_cts_na.alias('mac_stats_rx_cts_na'),
 df.radios.mac_stats_rx_data_mcast.alias('mac_stats_rx_data_mcast'),
 df.radios.mac_stats_rx_mgmt_mcast.alias('mac_stats_rx_mgmt_mcast'),
 df.radios.mac_stats_rx_ctl_mcast.alias('mac_stats_rx_ctl_mcast'),
 df.radios.mac_stats_rx_bcn_mbss.alias('mac_stats_rx_bcn_mbss'),
 df.radios.mac_stats_rx_ucast_obss.alias('mac_stats_rx_ucast_obss'),
 df.radios.mac_stats_rx_bcn_obss.alias('mac_stats_rx_bcn_obss'),
 df.radios.mac_stats_rx_tsp_timeout.alias('mac_stats_rx_tsp_timeout'),
 df.radios.mac_stats_tx_bcn_cancel.alias('mac_stats_tx_bcn_cancel'),
 df.radios.mac_stats_rx_no_delimiter.alias('mac_stats_rx_no_delimiter'),
 df.radios.mac_stats_rxf1ovfl.alias('mac_stats_rxf1ovfl'),
 df.radios.mac_stats_miss_bcn_dbg.alias('mac_stats_miss_bcn_dbg'),
 df.radios.mac_stats_pmqovfl.alias('mac_stats_pmqovfl'),
 df.radios.mac_stats_rx_hwprobe_req.alias('mac_stats_rx_hwprobe_req'),
 df.radios.mac_stats_rx_hwprobe_req_qoverflow.alias('mac_stats_rx_hwprobe_req_qoverflow'),
 df.radios.mac_stats_tx_hwprobe_resp.alias('mac_stats_tx_hwprobe_resp'),
 df.radios.mac_stats_tx_hwprobe_resp_failed.alias('mac_stats_tx_hwprobe_resp_failed'),
 df.radios.mac_stats_tx_hwprobe_resp_drop_timeout.alias('mac_stats_tx_hwprobe_resp_drop_timeout'),
 df.radios.mac_stats_tx_rts_fail.alias('mac_stats_tx_rts_fail'),
 df.radios.mac_stats_tx_ucast.alias('mac_stats_tx_ucast'),
 df.radios.mac_stats_tx_in_rts_txop.alias('mac_stats_tx_in_rts_txop'),
 df.radios.mac_stats_rx_block_ack.alias('mac_stats_rx_block_ack'),
 df.radios.mac_stats_tx_block_ack.alias('mac_stats_tx_block_ack'),
 df.radios.mac_stats_phy_rx_glitch.alias('mac_stats_phy_rx_glitch'),
 df.radios.mac_stats_rx_drop2nd.alias('mac_stats_rx_drop2nd'),
 df.radios.mac_stats_rx_too_late.alias('mac_stats_rx_too_late'),
 df.radios.mac_stats_phy_bad_plcp.alias('mac_stats_phy_bad_plcp'),
 df.radios.tx_pkts_sent.alias('tx_pkts_sent'),
 df.radios.tx_pkts_sent_mcast.alias('tx_pkts_sent_mcast'))
                   
    
    #convert time
    convert_time = udf(lambda x: datetime.utcfromtimestamp(x/1000000).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')[:16], StringType())
    concat_udf = F.udf(lambda cols: "_".join([x if x is not None else "*" for x in cols]), StringType())
    nwlans_udf = udf(lambda x: len(x), IntegerType())
    
    df = df.withColumn("id_dev", concat_udf(F.array("id", "band")))            .withColumn('nwlans', nwlans_udf('wlans'))            .withColumn('time', convert_time('terminator_timestamp'))
    
    df = df.filter(df.nwlans!=0)            .filter(df.dev!='r2')

    return df 


# In[ ]:



date = '2021-08-24'

inpath = "s3://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/dt="+date+"/*" 

dfin = spark.read.parquet(inpath).repartition(500)
df = todf(dfin)
df.persist().count()
df.repartition(500).write.parquet('s3://mist-data-science-dev/jing/radio_subset2/'+date+'_a')


# # Get One Day One Mac Data

# In[ ]:



mac = '5c-5b-35-ce-b2-a7'
band = '5'

if ':' in mac:
    mac = mac.replace(':','-')
if '-' not in mac:
    mac = mac[:2]+'-'+mac[2:4]+'-'+mac[4:6]+'-'+mac[6:8]+'-'+mac[8:10]+'-'+mac[10:12] 

id_dev = mac + '_' + band

date0 = '2021-05-30'
date1 = '2021-05-30'
#from 2019-8-14, data filtered by wlans under radio
start_time = datetime.strptime(date0 + 'T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
end_time = datetime.strptime(date1 + 'T23:59:59Z', '%Y-%m-%dT%H:%M:%SZ')
    
while start_time <= end_time:
    date = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')[:10]
    inpath = "s3://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/dt="+date+"/*" 
    # inpath = "gs://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/dt="+date+"/*"
    print (inpath)
    try: 
        dfin = spark.read.parquet(inpath)
        dfin = dfin.filter(dfin.id == mac)
        df = todf(dfin)
        df = df.filter(df.id_dev == id_dev)
    except Exception as e:
        print (inpath, ' schema changed or not exist')
    start_time = start_time + timedelta(minutes=1440)

    


# In[ ]:


df.select('time','num_active_clients','re_init','channel','uptime','rx_mgmt').sort('time').show(1500 )


# In[ ]:





# In[ ]:



#data_prep
def f():
    import numpy as np
    import pandas as pd
    import sklearn

    import pyspark

    #np.printoptions(precision=2, suppress=True)
    np.set_printoptions(suppress=True)

    from IPython.core.interactiveshell import InteractiveShell
    InteractiveShell.ast_node_interactivity = "all"

    import datetime
    from datetime import datetime, timedelta

    import pyspark
    from pyspark.sql import SparkSession, Row
    from pyspark.sql.types import StringType, IntegerType, ArrayType
    import pyspark.sql.functions as fn
    from pyspark.sql.functions import udf, size, avg, count, col
    from operator import itemgetter
    import json

    from pyspark.ml.clustering import KMeans
    from pyspark.ml.feature import VectorAssembler

    from pyspark.sql.functions import mean, stddev, col 

    from pyspark.ml.linalg import Vectors
    from pyspark.ml.stat import Correlation

    from sklearn.datasets import load_iris
    from sklearn.linear_model import LogisticRegression

    from pyspark.sql import functions as F

    from pyspark.sql.functions import udf, struct, explode
    from pyspark.sql.types import IntegerType

    import datetime
    from datetime import datetime, timedelta
    


    #Run every evening at 9:00pm
    import datetime
    from datetime import datetime
    day = datetime.strftime((datetime.today() - timedelta(minutes=1440)).date(),"%Y-%m-%d")
    #day = datetime.strftime(datetime.today().date(),"%Y-%m-%d")
    start_time = datetime.strptime(day + 'T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
    end_time = datetime.strptime(day + 'T23:59:59Z', '%Y-%m-%dT%H:%M:%SZ')

    '''
    start_time = datetime.strptime('2019-07-21T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
    end_time = datetime.strptime('2019-07-21T23:59:59Z', '%Y-%m-%dT%H:%M:%SZ')
    '''

    while start_time <= end_time:
        date = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')[:10]
        inpath = "s3a://mist-secor-production/ap-stats-analytics/ap-stats-analytics-production/dt="+date+"/*" 
        print inpath
        df = spark.read.parquet(inpath)
        df = todf(df)
        #df = df.drop('per_antenna_rssi','wlans','total_client_count','active_client_count','re_init_throttle')
        df.persist().count()
        df.coalesce(100).write.parquet('s3://mist-data-science-dev/jing/radio_subset2/'+date)
        df.unpersist()
        start_time = start_time + timedelta(minutes=1440)

'''
#crontab.py
import time, data_prep
while True:
    data_prep.f()
    #time.sleep(86400)  #daily update
'''

'''
/usr/bin/spark-submit /home/hadoop/crontab.py
'''


# In[22]:


#Add additional fields


# In[ ]:



def adddf(df_in):

    df = df_in.filter(df_in.radios.isNotNull())             .filter(df_in.wlans[0].isNotNull())             .filter(df_in.delta==True)
        
    df = df.select( 'id',
                   'terminator_timestamp',
                     explode(df.radios).alias('radios')) 

    df = df.select('id',
                   col('terminator_timestamp').alias('terminator_timestamp1'),
                   df.radios.bandwidth.alias('bandwidth'),
                   df.radios.radio_missing.alias('radio_missing'),
                   df.radios.band.alias('band'))

    concat_udf = F.udf(lambda cols: "_".join([x if x is not None else "*" for x in cols]), StringType())
    df = df.withColumn("id_dev1", concat_udf(F.array("id", "band"))).drop('id','band')  
    
    return df 


# In[ ]:


start_time = datetime.strptime('2019-07-04T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
end_time = datetime.strptime('2019-07-17T23:59:59Z', '%Y-%m-%dT%H:%M:%SZ')

while start_time <= end_time:
    date = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')[:10]
    addinpath = "s3a://mist-secor-production/ap-stats-analytics/ap-stats-analytics-production/dt="+date+"/*" 
    print addinpath
    dfadd = spark.read.parquet(addinpath)
    dfadd = adddf(dfadd)
    inpath = 's3://mist-data-science-dev/jing/radio_subset/'+date
    df = spark.read.parquet(inpath)
    df = df.join(dfadd, (df.id_dev==dfadd.id_dev1)&(df.terminator_timestamp==dfadd.terminator_timestamp1), 'inner').drop('id_dev1')
    df.persist().count()
    df.coalesce(100).write.parquet('s3://mist-data-science-dev/jing/radio_subset1/'+date)
    df.unpersist()
    start_time = start_time + timedelta(minutes=1440)

    


# In[ ]:


#Run single radio single field


# In[ ]:


inpath = "s3a://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/dt=2020-11-18/hr=1*"
df = spark.read.parquet(inpath)
df0 = df.filter(df.id=='5c-5b-35-be-65-a4')
df0 = todf(df0)
df0 = df0.filter(df0.band=='5')


# In[ ]:


start_time = datetime.strptime('2019-11-23T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
end_time = datetime.strptime('2019-11-29T23:59:59Z', '%Y-%m-%dT%H:%M:%SZ')
flag = True
while start_time <= end_time:
    date = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')[:10]
    inpath = 's3://mist-data-science-dev/jing/radio_subset2/'+date
    dfin = spark.read.parquet(inpath)                 .filter(col('id')=='5c-5b-35-af-02-fc')
    print (inpath)
    if flag:
        df = dfin
        flag = False
    else:
        df = df.union(dfin)
    start_time = start_time + timedelta(minutes=1440)

df.toPandas().to_csv('target_5c-5b-35-af-02-fc.csv')    


# In[ ]:





# In[ ]:





# In[ ]:


start_time = datetime.strptime('2019-07-04T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
end_time = datetime.strptime('2019-07-17T23:59:59Z', '%Y-%m-%dT%H:%M:%SZ')

while start_time <= end_time:
    date = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')[:10]
    inpath = 's3://mist-data-science-dev/jing/radio_subset1/'+date
    df = spark.read.parquet(inpath)
    df = df.select('id',
 'site_id',
 'org_id',
 'model',
 'time',
 'terminator_timestamp',
 'config_time',
 'nwlans',
 'uptime',
 'firmware_version',
 'pwr_available',
 'rx_mgmt',
 'tx_mgmt',
 'tx_power',
 'bandwidth',
 'max_tx_power',
 'utilization_tx',
 'utilization_rx_in_bss',
 'utilization_rx_other_bss',
 'radio_missing',
 're_init',
 'tx_errors',
 'tx_failed_arp',
 'tx_drop_arp',
 'tx_psblk_fifo',
 'tx_toss',
 'tx_toss_arp',
 'tx_toss_bcmc',
 'tx_toss_unicast',
 'tx_bcmc2unicast',
 'tx_bcmc2unicast_client',
 'tx_bcn_intr',
 'tx_phy_err',
 'tx_failed',
 'tx_retries',
 'tx_retried',
 'rx_dups',
 'rx_retried',
 'tx_bcmc2unicast_bytes',
 'tx_bcmc2unicast_client_bytes',
 'tx_mgmt_dropped',
 'rx_fifo_overflow',
 'rx_hl_fifo_overflow',
 'rx_hwprobe_req',
 'rx_hwprobe_req_qoverflow',
 'tx_hwprobe_resp',
 'tx_hwprobe_resp_failed',
 'tx_hwprobe_resp_drop_timeout',
 'bcn',
 'rx_probe_req_bc',
 'rx_probe_req_non_bc',
 'tx_probe_resp_sw',
 'rx_probe_req_rand',
 'rx_bytes',
 'rx_pkts',
 'tx_bytes',
 'tx_pkts',
 'rx_errors',
 'dev',
 'band',
 'channel',
 'num_active_clients',
 'id_dev'
)
    #df.persist().count()
    df.write.parquet('s3://mist-data-science-dev/jing/radio_subset2/'+date)
    #df.unpersist()
    start_time = start_time + timedelta(minutes=1440)    


# # explode both radios and wlans

# In[ ]:



#spark = SparkSession.builder.getOrCreate()


    df = df_in.filter(df_in.radios.isNotNull())             .filter(df_in.delta==True)             .filter(df_in.wlans[0].isNotNull())

    df = df.select( 'id',
                     'site_id',
                     'org_id',
                   'model',
                   'terminator_timestamp',
                   'config_time',
                   'total_client_count',
                   'active_client_count',
                   'delta',
                   #'wlans',
                   'uptime',
                   'firmware_version', 
                   'pwr_available',
                   'wlans',
                     explode(df.radios).alias('radios'))\
                .repartition(1000)


    df = df.select('id',
                   'site_id',
                   'org_id',
                   'terminator_timestamp',
                   df.radios.dev.alias('dev'),
                   df.radios.band.alias('band'),
                   df.radios.num_active_clients.alias('num_active_clients'),
                   df.radios.num_active_clients.alias('num_clients'),
                  df.radios.wlans.alias('radio_wlans'),
                    explode(df.wlans).alias('wlans'))
    
    df = df.select('id',
                   'site_id',
                   'org_id',
                   'terminator_timestamp',
                    'dev',
                    'band',
                    'num_active_clients',
                    'num_clients',
                    'radio_wlans',
                    df.wlans.num_clients.alias('wlan_num_clients'),
                    df.wlans.id.alias('wlan_id'),
                    df.wlans.radio.alias('wlan_radio'))
    
    concat = udf(lambda x: x[0]+'+'+x[1], StringType())
    df = df.withColumn('ap_wlan', concat(struct('id','wlan_id')))             .withColumn('wlan_site', concat(struct('wlan_id','site_id')))    
    
    nwlans_udf = udf(lambda x: len(x), IntegerType())
    df = df.withColumn("id_dev", concat_udf(F.array("id", "band")))            .withColumn('n_radio_wlans', nwlans_udf('radio_wlans'))
    
    df = df.filter(df.n_radio_wlans!=0)            .filter(df.dev!='r2')

    df = df.withColumnRenamed('id','ap')
    dfg_radio = df.groupby('ap','site_id','org_id').agg(avg('num_clients').alias('avg_clients'))
    dfg_wlan = df.groupby('ap_wlan','wlan_site','wlan_id').agg(avg('wlan_num_clients').alias('avg_clients'))
    
    flag = True
    for scope in ['ap','site_id','org_id']:
        dfg_agg = dfg_radio.groupby(scope).agg(sum_('avg_clients').alias('cnt_clients'))
        dfg_agg = dfg_agg.withColumn('scope', lit(scope))
        if flag:
            df_scope = dfg_agg
            flag = False
        else:
            df_scope = df_scope.union(dfg_agg)

    for scope in ['ap_wlan','wlan_site','wlan_id']:
        dfg_agg = dfg_wlan.groupby(scope).agg(sum_('avg_clients').alias('cnt_clients'))
        dfg_agg = dfg_agg.withColumn('scope', lit(scope))
        df_scope = df_scope.union(dfg_agg)
            

In [82]: df_scope.persist().count()
Out[82]: 1668056
    
In [85]: df_scope.filter(df_scope.scope=='site_id').count()
Out[85]: 24324                                                                  

In [86]: df_scope.filter(df_scope.scope=='org_id').count()
Out[86]: 3123                                                                   

In [87]: df_scope.filter(df_scope.scope=='wlan_id').count()
Out[87]: 16117                                                                  

In [88]: df_scope.filter(df_scope.scope=='ap_wlan').count()
Out[88]: 1237132                                                                

In [89]: df_scope.filter(df_scope.scope=='wlan_site').count()
Out[89]: 107585                                                                 

In [90]: df_scope.filter(df_scope.scope=='id').count()
Out[90]: 279775             
            
            
# rdd = df.rdd \
#     .map(lambda x: x.asDict()) \
#     .map(lambda x: {(x.get('id', 'null'),
#                  x.get('wlan_id', 'null'),
#                  x.get('site_id', 'null'),
#                  x.get('org_id', 'null'),
#                  x.get('ap_wlan', 'null'),
#                  x.get('wlan_site', 'null')): x})

ap_list = ["5c-5b-35-2e-36-83", "5c-5b-35-3f-46-27", "5c-5b-35-8e-bc-c0", "5c-5b-35-3e-d0-d9", "d4-20-b0-83-37-0c","5c-5b-35-ae-37-e1",
"d4-dc-09-ad-eb-02", "5c-5b-35-2e-bd-15", "5c-5b-35-8e-9f-dd", "5c-5b-35-8e-87-0f","5c-5b-35-3e-cb-de","d4-20-b0-01-78-95",
"5c-5b-35-3e-d3-13", "d4-dc-09-af-6e-b4", "5c-5b-35-3e-2f-f8", "5c-5b-35-3f-1a-58", "5c-5b-35-8f-2c-23", "5c-5b-35-3e-d7-41",
"d4-20-b0-84-8a-44", "5c-5b-35-ae-3d-b8", "d4-20-b0-00-eb-1e"]
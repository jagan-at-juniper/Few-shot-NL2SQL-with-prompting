import numpy as np
import pandas as pd
import sklearn

import pyspark

import datetime
from datetime import datetime, timedelta
import pyspark
from pyspark.shell import spark
from pyspark.sql import SparkSession, Row, Window
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
from pyspark.sql.functions import lag, col


def todf(df_in):
    df = df_in.filter(df_in.radios.isNotNull()).filter(df_in.delta == True).filter(df_in.wlans[0].isNotNull())
    top_5 = ["d4-20-b0-80-72-38", "d4-20-b0-80-02-f3", "d4-20-b0-81-2c-f5", "d4-20-b0-80-58-6b", "d4-20-b0-80-93-2b"]
    df = df.filter(df.id.isin(top_5))
    df = df.select('id',
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
                   explode(df.radios).alias('radios')) \
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

    # convert time
    convert_time = udf(lambda x: datetime.utcfromtimestamp(x / 1000000).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')[:16],
                       StringType())
    concat_udf = F.udf(lambda cols: "_".join([x if x is not None else "*" for x in cols]), StringType())
    nwlans_udf = udf(lambda x: len(x), IntegerType())
    df = df.filter(df.band == 5)
    df = df.filter(df.model == "AP41-US")

    df = df.withColumn("id_dev", concat_udf(F.array("id", "band"))).withColumn('nwlans',
                                                                               nwlans_udf('wlans')).withColumn('time',
                                                                                                               convert_time(
                                                                                                                   'terminator_timestamp'))

    df = df.filter(df.nwlans != 0).filter(df.dev != 'r2')

    # df = df.select('id',
    #                  'site_id',
    #                  'org_id',
    #                  'model',
    #                  'firmware_version',
    #                  'band',
    #                  'num_clients',
    #                  'tx_pkts',
    #                  'ampdutx_tx_ampdu_succ',
    #                  'time')

    return df


#apid = "d4-20-b0-81-46-31"
date1 = '2022-10-11'
date2 = '2022-10-11'

#from 2019-8-14, data filtered by wlans under radio
start_time = datetime.strptime(date1+'T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
end_time = datetime.strptime(date2+'T23:59:59Z', '%Y-%m-%dT%H:%M:%SZ')

df_list = []

file_list = []
date = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')[:10]

# for hour in range(10,13):
#     inpath = "s3a://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/dt=" + date + "/hr=" + str(hour)
#     file_list.append(inpath)

while start_time <= end_time:
    date = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')[:10]
    hour = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')[11:13]
    inpath = "s3a://mist-secorapp-production/ap-stats-analytics/ap-stats-analytics-production/dt="+date+"/hr=*"
    print (inpath)
    try:
        #dfin = spark.read.parquet(*file_list).repartition(2000)
        dfin = spark.read.parquet(inpath).repartition(2000)
        df = todf(dfin).persist()
        #sort the dataframe by id and then by time
        df = df.orderBy(['id','time'])
        #add a new column for the previous value
        w = Window().partitionBy("id").orderBy(col("time"))
        df = df.select("*").withColumn('previous_ampdutx_tx_ampdu_succ', lag("ampdutx_tx_ampdu_succ",1).over(w))
        #calculate the difference between current and previous value
        df = df.withColumn("change_in_prev_ampdu_succ", col("ampdutx_tx_ampdu_succ") - col("previous_ampdutx_tx_ampdu_succ"))
        #check if the value is zero for 24 hours
        df = df.withColumn('zero_exists',(F.sum('change_in_prev_ampdu_succ').over(Window.partitionBy('id')) == 0))
        #get only those data for which value is zero
        df = df.filter(df.zero_exists == True)
        df.repartition(500).write.parquet('s3://mist-data-science-dev/amruta/radio_issues_24hours/' + date)

        #site id window
        df = df.withColumn("site_txpkts", (F.mean("tx_pkts").over(Window.partitionBy("site_id"))))
        df = df.withColumn("site_ampdutx_tx_ampdu_succ", (F.mean("ampdutx_tx_ampdu_succ").over(Window.partitionBy("site_id"))))
        df.orderBy(["site_id", "id", "time"]).repartition(500).write.parquet('s3://mist-data-science-dev/amruta/site_id_mean_computed/' + date)

        df = df.withColumn("site_num_clients",
                           (F.mean("num_clients").over(Window.partitionBy("site_id"))))
        df = df.withColumn("avg_txpkts",
                           (F.mean("tx_pkts").over(Window.partitionBy("id"))))

        selected_candidates = df.filter(df.site_ampdutx_tx_ampdu_succ > 2000).filter(df.avg_txpkts > 10000).filter(
            df.site_txpkts > 5000)
        selected_candidates.orderBy(["site_id", "id", "time"]).repartition(500).write.parquet('s3://mist-data-science-dev/amruta/selected_candidates/' + date)


    except Exception as e:
        print (e)
    start_time = start_time + timedelta(minutes=1440)


##########################parquet is written to S3, now read and proccess

dfin = spark.read.parquet('s3a://mist-data-science-dev/amruta/radio_issues_3hr/'+date).repartition(2000)
df = dfin.select('id',
                   'site_id',
                   'org_id',
                   'model',
                   'firmware_version',
                   'band',
                 'num_clients',
                 'tx_pkts',
                 'ampdutx_tx_ampdu_succ',
                 'time')

pandas_df = df.toPandas()
#pandas_df = pd.concat(df_list)
pandas_df = pandas_df.sort_values(['id', 'time'], ascending=[True, False])
pandas_df['previous_ampdutx_tx_ampdu_succ'] = pandas_df.groupby('id')['ampdutx_tx_ampdu_succ'].shift(1)
#change in the value = current - previous row
pandas_df['change_in_prev_ampdu_succ'] = pandas_df['ampdutx_tx_ampdu_succ'] - pandas_df['previous_ampdutx_tx_ampdu_succ']

#check if the change in previous ampdu succ is zero, that means there is no success
pandas_df['zero_exists'] = pandas_df['id'].map(pandas_df.groupby('id').apply(lambda x: x['change_in_prev_ampdu_succ'].eq(0).any()))
pandas_df = pandas_df[pandas_df['zero_exists'] ==  True] #get only those APs for which one of the field is zero
pandas_df.to_csv("amruta/zero_ampdu_sample_aps_3hours.csv")
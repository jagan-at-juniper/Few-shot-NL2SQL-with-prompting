import json
import csv
import re
import requests
import time
from kafka import KafkaProducer
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
from datetime import datetime,timedelta

AP_REGEX = re.compile('[^A-Fa-f0-9]')

ENV = 'production'
BATCH_COUNT = 10

ENV_CONFIG = {
    'ENV': ENV,
    'CLOUD_PROVIDER': 'gcp',
    'kafka.port': '6667',
    "kafka.hosts": [
        "kafka-000-{}.mist.pvt".format(ENV),
        "kafka-001-{}.mist.pvt".format(ENV),
        "kafka-002-{}.mist.pvt".format(ENV)
  ]
}


PAPI_URL = 'http://papi-internal-{}.mist.pvt'.format(ENV)
RADIO_REINIT_URL = "{}/internal/devices/{}/cmd/radio_reinit"

def json_value_serializer(v):
    return json.dumps(v).encode('utf-8')

# Submit POST to PAPI
def post_to_papi(url, data):

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "X-FROM": "Walmart_MarvisScript"
    }

    r = requests.post(
        url=url,
        json=data,
        headers=headers
    )

    if r.status_code != 200:
        print(r.json)
        return False
    return True

def get_message_producer():
    """
    This API can only be called from driver node.
    :return:
    """

    # Set environment and provider
    CLOUD_ENV = '{provider}-{env}'.format(provider=ENV_CONFIG['CLOUD_PROVIDER'], env=ENV_CONFIG['ENV'])

    # kafka versions mapping
    kafka_versions = {
        'aws-staging': (2, 1, 1),
        'aws-eu': (0, 10, 1),
        'aws-production': (0, 10, 1),
        'aws-use1prod2': (2, 1, 1),
        'gcp-staging': (2, 1, 1),
        'gcp-production': (2, 1, 1)
    }
    kafka_version = kafka_versions.get(CLOUD_ENV, (2, 1, 1))

    kafka_port = ENV_CONFIG.get('kafka.port', '6667')
    kafka_hosts = ENV_CONFIG.get('kafka.hosts', '')

    if kafka_hosts:
        bootstrap_servers = ['{}:{}'.format(h, kafka_port) for h in kafka_hosts]

        return KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=json_value_serializer,
                             api_version=kafka_version,
                             acks=1)
    else:
        return None



def action_notification(action_list):
    '''
    a list of high confidence and high severity events need to be notified
    for down stream process to take actions:
       notify user
       take auto actions: auto ap reboot, etc

    :param event_list:
    :return:
    '''
    if action_list is None or len(action_list) < 1:
        return

    topic = 'marvis-action-' + ENV
    producer = get_message_producer()

    for act in action_list:
        producer.send(topic, act)

    producer.flush()


def compose_action(org_id, site_id, ap_id, dev, action, ts):

    return {
        "row_key": "{}_{}_{}_{}".format(site_id, ap_id, action, ts),
        "org_id": org_id,
        "site_id": site_id,
        "display_entity_id": ap_id,
        "display_entity_type": "ap",
        "action": action,
        "target_id": '{}_{}_{}_{}'.format(site_id, ap_id, '5', dev), #"afc257ac-6fbf-47ac-9831-97d635443bc3_d4dc09af5a78_5_r0",
        "target_type": "radio",
        "entity_detail": {
            "band": "5",
            "dev": "r0"
        },
        "note": """{"action_entity": "radio"}""",
        "when": ts, #1638367832000,
        "who": "MarvisScript"
    }


def format_ap_id(input_ap_id):
    """
    lambda input_ap_id: re.sub('[^A-Fa-f0-9]', '', input_ap_id)
    :param input_ap_id:
    :return:
    """

    return AP_REGEX.sub('', input_ap_id).lower()


def call_papi(action_list):
    for a in action_list:
        endpoint = RADIO_REINIT_URL.format(PAPI_URL, a)
        data = {'radio': 'r0'}
        ans = post_to_papi(endpoint, data)
        if not ans:
            print('failed: {}'.format(a))


def bad_radio_detection(date_day, date_hour):

    spark = SparkSession \
        .builder \
        .appName("ap-zero-clients") \
        .getOrCreate()

    fs = "gs" if ENV_CONFIG.get('CLOUD_PROVIDER') == "gcp" else "s3"

    # detect_time = datetime.now() - timedelta(hours=1)
    # date_day = detect_time.strftime("%Y-%m-%d")
    # date_hour = detect_time.strftime("%H")

    s3_bucket = "{fs}://mist-secorapp-{env}/ap-stats-analytics/ap-stats-analytics-{env}/".format(fs=fs, env=ENV)
    s3_bucket += "dt={date}/hr={hr}/*.parquet".format(date=date_day, hr=date_hour)
    print(s3_bucket)

    df= spark.read.parquet(s3_bucket)
    df.printSchema()

    # Radio
    df_radio = df.filter("uptime>86400") \
        .select("org_id", "site_id", "id", "hostname", "firmware_version", "model",
                F.col("when").alias("timestamp"),
                F.explode("radios").alias("radio")
                ) \
        .filter("radio.dev != 'r2' and radio.bandwidth>0 and not radio.radio_missing and radio.band==5") \
        .withColumn("num_wlans", F.size("radio.wlans")) \
        .withColumn("bcn_per_wlan", F.col("radio.interrupt_stats_tx_bcn_succ")/F.col("num_wlans"))

    df_radio_nf_g = df_radio \
        .select("org_id", "site_id", "id", "hostname", "firmware_version", "model", "radio.*", "num_wlans", "bcn_per_wlan") \
        .groupBy("org_id", "site_id", "id","hostname", "firmware_version", "model", "band") \
        .agg(
        F.max("num_clients").alias("max_num_clients"),
        F.max('tx_phy_err').alias("max_tx_phy_err"),
        F.max("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_max"),
        F.min("interrupt_stats_tx_bcn_succ").alias("interrupt_stats_tx_bcn_succ_min"),
        F.min("bcn_per_wlan").alias("bcn_per_wlan_min"),
        F.avg("bcn_per_wlan").alias("bcn_per_wlan"),
        F.max("num_wlans").alias("num_wlans")
    )

    Filter_query_1 = "band==5 and max_num_clients <1 and max_tx_phy_err>0 and num_wlans>0 and bcn_per_wlan < 500"
    df_radio_nf_problematic = df_radio_nf_g.filter(Filter_query_1).persist()

    return df_radio_nf_problematic


def save_df_to_fs(df_radio_nf_problematic, date_day, date_hr):

    fs = "gs" if ENV_CONFIG.get('CLOUD_PROVIDER') == "gcp" else "s3"
    s3_path = "{fs}://mist-data-science-dev/shirley/aps-no-client-all/dt={dt}/hr={hr}" \
        .format(fs=fs, dt=date_day.replace("[", "").replace("]", ""), hr=date_hr)

    dt_str = '{}_{}'.format(date_day, date_hr)
    df_radio_nf_problematic.withColumn('recovery_time', F.lit(dt_str)).coalesce(1).write.save(s3_path,
                                                   format='csv',
                                                   mode='overwrite',
                                                   header=True)

def check_org_and_model(df_radio_nf_problematic):
    """

    :return:
    """
    df_radio_nf_problematic_g1 = df_radio_nf_problematic.groupBy("org_id").agg(
        F.countDistinct("id").alias("aps"))
    df_radio_nf_problematic_g1.show(truncate=False)

    df_radio_nf_problematic_g2 = df_radio_nf_problematic.groupBy("org_id", "site_id").agg(
        F.countDistinct("id").alias("aps")).orderBy(F.col("aps").desc())
    df_radio_nf_problematic_g2.show(truncate=False)

    df_radio_nf_problematic.groupby("firmware_version" , "model") \
        .count().orderBy(F.col("count").desc()) \
        .show(truncate=False)


if __name__ == "__main__":

    # detection
    detect_time = datetime.now() - timedelta(hours=1)
    date_day = detect_time.strftime("%Y-%m-%d")
    date_hour = detect_time.strftime("%H")

    df_radio_nf_problematic = bad_radio_detection(date_day, date_hour)
    save_df_to_fs(df_radio_nf_problematic, date_day, date_hour)

    count = df_radio_nf_problematic.count()
    df_radio_nf_problematic.show()
    ap_id_list = df_radio_nf_problematic.select('id', 'model').collect()

    print('Total {} APs need be recovered'.format(count))

    check_org_and_model(df_radio_nf_problematic)
    if count > 500:
        print('Error: detected more than 200 APs')
        exit(1)

    # recover
    now = int(time.time() * 1000)
    cc = 0
    action_list = []

    for dd in ap_id_list:
        if dd['model'].startswith('AP43'):
            dev = 'r0'
            formated_ap = format_ap_id(dd['id'])
            endpoint = RADIO_REINIT_URL.format(PAPI_URL, formated_ap)

            action_json = formated_ap
            action_list.append(action_json)

            if len(action_list) == BATCH_COUNT:
                print('batch count = {} batch size = {}'.format(cc, len(action_list)))
                print(action_list)
                call_papi(action_list)
                action_list = []
                cc += 1
                time.sleep(20)

    if len(action_list) > 0:
        print('batch count = {} batch size = {}'.format(cc, len(action_list)))
        print(action_list)
        call_papi(action_list)
        action_list = []

#!/usr/bin/env python

import time
import json
import datetime

from kafka import SimpleClient, SimpleProducer, KafkaConsumer

ENV = "staging"
ENV = "production"

KAFKA_BROKERS = "kafka-001-{env}.mistsys.net:6667".format(env=ENV)

kafka_client = None

IPERFports = [80, 5050, 5051, 5055]
IPERFport_ind = 0
if ENV == "production":
    IPERFports = [80]


def get_kafka_client():
    global kafka_client
    if not kafka_client:
        kafka_client = SimpleClient(KAFKA_BROKERS, timeout=3)
    return kafka_client


def get_cmd_global(userSiteID="siteid", bands=None, org=""):
    if bands is None:
        bands = []
    rrmCmd = {
        "org_id": org,
        "site_id": userSiteID,
        "bands": bands,
        "command": "start-global-rrm",
        "reason": "rrmG-scheduled",
        "timestamp": int(time.time())
    }
    return rrmCmd


#
def get_cmd_acs(userOrgID="", userSiteID="siteid", apID="", band="", orgID=""):
    rrmCmd = {
        "HashKey": userSiteID,
        "InfoFromTerminator": {
            "ID": apID,
            "OrgID": orgID,
            "OriginalEncoding": 2,
            "RemoteAddr": "96.66.196.150:57307",
            "SiteID": userSiteID,
            "Terminator": "ep-terminator-000-production",
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        },
        "Topic": "rrm-acs-",
        "ap_id": apID,
        "band": band,
        "command": "acs-request-api",
        "site_id": userSiteID
    }
    rrmCmd = {"site_id": userSiteID,
              "org_id": userOrgID,
              "ap_id": apID,
              "band": band,
              "command": "acs-request-ap",
              "timestamp": time.time()
              }

    return rrmCmd




def encode_msg(results):
    try:
        return json.dumps(results)
    except:
        raise


class SimpleKafka(object):
    def __init__(self):
        self.client = get_kafka_client()

    def send(self, topic, msg):
        try:
            producer = SimpleProducer(self.client)
            producer.send_messages(topic, encode_msg(msg).encode())
            # producer.send_messages(topic, json.dumps(msg))

        except Exception as e:
            print(" KAFKA %s failed" % topic)
            print(str(e))

    # serialize the json to msg

    def listen(self, topics):
        consumer = KafkaConsumer(topics, bootstrap_servers=[KAFKA_BROKERS])
        return consumer


def publish_rrm_global(mes, topicName=""):
    simpleKafka = SimpleKafka()
    if not topicName:
        topicName = "rrm-global-{env}".format(env=ENV)

    print("Publishing {}".format([topicName, mes]))
    simpleKafka.send(topicName, mes)


def publish_rrm_acs(org, site, ap, band):
    rrmCmd = get_cmd_acs(org, site, ap, band)

    simpleKafka = SimpleKafka()
    topicName = "rrm-acs-{env}".format(env=ENV)

    print("Publishing {} {}".format(topicName, rrmCmd))
    simpleKafka.send(topicName, json.loads(json.dumps(rrmCmd)))


def publish_rrm_apscan():
    rrmCmd = {
        "ap": "5c5b350e0038",
        "band": "24",
        "bandwidth": 20,
        "channel": 1,
        "max_tx_power": 14,
        "noise_floor": -73,
        "num_clients": 0.0,
        "org": "f4d3653e-4aa3-11e5-8510-1258369c38a9",
        "radar": 0,
        "scan_channel": 1,
        "scan_congest_nonwifi": 0.1817946,
        "scan_congest_rxotherbss": 0.51374483,
        "scan_external_avg_rssi": -46.0,
        "scan_external_counts": 19.0,
        "scan_external_loud_bssid": "5c-5b-35-09-60-f1",
        "scan_external_loud_ssid": "ctaOffice",
        "scan_external_max_rssi": -46,
        "scan_insite_avg_rssi": -57.0,
        "scan_insite_counts": 1.0,
        "scan_insite_max_rssi": -57,
        "scan_noise_floor": -88.0,
        "scan_util_nonwifi": 0.12075259,
        "scan_util_rxotherbss": 0.35861632,
        "site": "f4d40c28-4aa3-11e5-8510-1258369c38a9",
        "timestamp": 1530072176,
        "tx_power": 0,
        "util_all": 0.77914435,
        "util_nonwifi": 0.18048215,
        "util_rxinbss": 2.7980973E-4,
        "util_rxotherbss": 0.5569891,
        "util_tx": 0.041393317
    }

    for apx in ['5c:5b:35:0e:06:e0', '5c:5b:35:0e:33:95', '5c:5b:35:0e:33:95']:
        rrmCmd['ap'] = apx.replace(":", "")
        topicName = "rrm-apscan-{env}".format(env=ENV)

        print("Publishing {} {}".format(topicName, rrmCmd))
        simpleKafka = SimpleKafka()
        simpleKafka.send(topicName, json.loads(json.dumps(rrmCmd)))


def publish_sle_capacity_anomaly():
    # apx =rrmCmd['ap'] = '5c-5b-35-3e-d2-dc'
    rrmCmd = {"util_nonwifi_std_base": 15.320930980455659, "channel": 36, "sle_coverage_base": 0.9404836892189347,
              "util_nonwifi_deviation": 6.945464386490732,
              "rssi_mean": -38.0,
              "sle_capacity_anomaly_score": -7.212070402318713E-6,
              "util_all_mean_base": 11.625250965691789, "band": "5",
              "util_cochannel_std": 3.0, "sle_coverage_anomaly": False,
              "util_cochannel_mean": 0.08846924756653607,
              "util_cochannel_std_base": 3.0, "events": [], "avg_nclients": 1.0
        , "sle_capacity_error": 0.9728948831677904, "interference_anomaly_score": 2.5003671791366626,
              "util_nonwifi_mean_base": 18.791522937359904, "sle_coverage_error": 0.9776450551874708,
              "util_cochannel_deviation": -9.970510250811154, "ap": "5c-5b-35-3e-5b-c2",
              "rssi_hist": {"-38": 62}, "error_rate": 0.008170286788769413, "sle_coverage": 1.0,
              "user_seconds_util_high": 0, "sle_coverage_base_std": 0.13874372331334434,
              "sle_coverage_anomaly_score": -0.06087771378006282, "interval": 62, "rssi_std_base": 13.09836466677554,
              "util_cochannel_mean_base": 0.09994534055039138, "interference_type": "nonwifi", "rssi_watermark": -72,
              "sle_capacity_base_std": 0.1, "anomaly": "", "user_seconds_rssi_weak": 0,
              "util_all_std": 13.613456546195188,
              "sle_capacity_impacted": 1.0, "user_seconds": 62, "power": 9, "user_seconds_util_high_impacted": 0,
              "timestamp": 1550339800, "rssi_std": 3.0,
              "org": "3831255c-e5f3-4b3f-9ab5-6cc640530e40", "bandwidth": 40, "util_nonwifi_mean": 88.24616680226723,
              "sle_capacity_anomaly": False, "util_nonwifi_std": 25.53173399929119,
              "rssi_mean_base": -45.19810685023973,
              "util_all_deviation": 0.0, "sle_capacity": 1.0, "site": "f1d45305-c977-46b0-807d-c11640dc1afa",
              "util_all_std_base": 8.153510430459704, "user_seconds_rssi_weak_impacted": 0,
              "sle_coverage_impacted": 1.0,
              "util_watermark": 20, "util_all_mean": 48.1438711211582, "time_since_last_interference_anomaly": 2400,
              "max_nclients": 1.0, "sle_capacity_base": 0.9999929834136085, "rssi_deviation": -0.549542407266915}

    rrmCmd = {"util_nonwifi_std_base": 18.393364097915306, "channel": 116,
              "sle_coverage_base": 0.9979865658339144,
              "util_nonwifi_deviation": 6.044354478393778, "rssi_mean": -53.0,
              "sle_capacity_anomaly_score": -2.2290013732501936E-13,
              "util_all_mean_base": 7.209716116794135, "band": "5",
              "util_cochannel_std": 3.0, "sle_coverage_anomaly": False,
              "util_cochannel_mean": 0.0029010183425270952, "util_cochannel_std_base": 3.0,
              "events": [], "avg_nclients": 1.0, "sle_capacity_error": 0.49409625926889705,
              "interference_anomaly_score": 2.5843640799825724,
              "util_nonwifi_mean_base": 10.016168333343812,
              "sle_coverage_error": 0.49409625926889705,
              "util_cochannel_deviation": -9.999032993885825, "ap": "5c-5b-35-3e-5c-f8",
              "rssi_hist": {"-53": 124}, "error_rate": 0.0, "sle_coverage": 1.0,
              "user_seconds_util_high": 0, "sle_coverage_base_std": 0.1,
              "sle_coverage_anomaly_score": -0.004074983625791466, "interval": 62,
              "rssi_std_base": 9.604057271645154, "util_cochannel_mean_base": 0.17772447565874872,
              "interference_type": "nonwifi", "rssi_watermark": -72, "sle_capacity_base_std": 0.1,
              "anomaly": "", "user_seconds_rssi_weak": 0, "util_all_std": 10.567005124620882,
              "sle_capacity_impacted": 1.0, "user_seconds": 124, "power": 10,
              "user_seconds_util_high_impacted": 0, "timestamp": 1550442000,
              "rssi_std": 3.0, "org": "3831255c-e5f3-4b3f-9ab5-6cc640530e40", "bandwidth": 40,
              "util_nonwifi_mean": 70.4597131172816, "sle_capacity_anomaly": False,
              "util_nonwifi_std": 16.86301750136971, "rssi_mean_base": -48.754071729672646, "util_all_deviation": 0.0,
              "sle_capacity": 1.0,
              "site": "f1d45305-c977-46b0-807d-c11640dc1afa", "util_all_std_base": 9.540772733834745,
              "user_seconds_rssi_weak_impacted": 0,
              "sle_coverage_impacted": 1.0, "util_watermark": 20, "util_all_mean": 42.42335050056378,
              "time_since_last_interference_anomaly": 3000, "max_nclients": 1.0,
              "sle_capacity_base": 0.9999999999998899,
              "rssi_deviation": 0.442097350133777}

    rrmCmd['timestamp'] = time.time()
    # if apx in ['5c-5b-35-3e-ce-18', '5c-5b-35-3e-d2-dc']:
    # rrmCmd['ap'] = apx.replace(":", "")
    topicName = "sle-capacity-anomaly-{env}".format(env=ENV)

    print("Publishing {} {}".format(topicName, rrmCmd))
    simpleKafka = SimpleKafka()
    simpleKafka.send(topicName, json.loads(json.dumps(rrmCmd)))


def publish_acs_dfs():
    rrmCmd = {"org_id":"b4e16c72-d50e-4c03-a952-a3217e231e2c",
              "site_id":"35670395-458f-4ecd-8bdf-1eda949595c2",
              "ap_id":"5c-5b-35-3e-18-ba","band":"5",
              "Topic":"rrm-acs-","command":"acs-request-dfs",
              "reason":"radar-detected",
              "text":"Current Channel 100/40 Target Channel 44/40",
              "CurrChanSet":"100/40",
              "TargetChanSet":"44/40","timestamp":"2019-05-28T22:03:24.711935333Z"
              }
    rrmCmd = {"org_id":"cfa342ce-8ff4-11e6-a765-0242ac110004",
              "site_id":"d5d33375-ea5b-4f0c-accc-e9b5a193cec6",
              "ap_id":"5c-5b-35-0e-04-10","band":"5","Topic":"rrm-acs-",
              "command":"acs-request-dfs","reason":"radar-detected",
              "text":"Current Channel 52/40 Target Channel 36/40",
              "CurrChanSet":"52/40","TargetChanSet":"36/40",
              "timestamp":"2019-05-28T19:36:40.720007334Z"}

    rrmCmd = {"org_id":"cfa342ce-8ff4-11e6-a765-0242ac110004","site_id":"d5d33375-ea5b-4f0c-accc-e9b5a193cec6","ap_id":"5c-5b-35-0e-04-10","band":"5","Topic":"rrm-acs-","command":"acs-request-dfs","reason":"radar-detected","text":"Current Channel 52/40 Target Channel 36/40","CurrChanSet":"52/40","TargetChanSet":"36/40","timestamp":"2019-05-28T19:36:40.720007334Z"}
    rrmCmd['timestamp'] = time.time()

    # rrmCmd['ap'] = apx.replace(":", "")
    topicName = "rrm-acs-{env}".format(env=ENV)

    print("Publishing {} {}".format(topicName, rrmCmd))
    simpleKafka = SimpleKafka()
    mes = rrmCmd
    simpleKafka.send(topicName, mes)
    # return rrmCmd


def publish_mes():
    topicName = 'ap-outage-staging'
    mes = {
        "AP": "5c-5b-35-0e-00-38",
        "ConnectionID": "a3105fea-48a7-11e8-a4b4-6ca504dbb815",
        "LastActivityTime": "2018-04-25T18:43:57.248355037Z",
        "OrgID": "f4d3653e-4aa3-11e5-8510-1258369c38a9",
        "OutageRemoteAddress": "73.92.124.103:2925",
        "OutageTime": "2018-04-25T18:44:56.626776584Z",
        "Recovery": False,
        "SiteID": "f4d40c28-4aa3-11e5-8510-1258369c38a9",
        "Version": 2
    }
    mes['Recovery'] = True
    simpleKafka = SimpleKafka()
    print("Publishing {} {}".format(topicName, mes))
    simpleKafka.send(topicName, json.loads(json.dumps(mes)))


def test_global():
    sites = []
    # staging

    if ENV == "staging":
        # sites.append("f4d40c28-4aa3-11e5-8510-1258369c38a9")
        sites.append(
            # ( "f7dd72bf-ffc3-485a-b5ae-c3c5d614c8d1", "fb20cd8a-be6f-4acd-9c60-c6a26bf531ad")
            # ("f4d3653e-4aa3-11e5-8510-1258369c38a9", "29d2648e-e66b-11e5-a6cd-0242ac110003")
            # ("bf8b581c-eadd-42dc-945c-d278c75f73c5", "76db44ae-8e5b-4cd8-b802-4e6769087003")
            ("6748cfa6-4e12-11e6-9188-0242ac110007", "67970e46-4e12-11e6-9188-0242ac110007")
        )

    if ENV == "production":
        # production
        sites = [  # "c8a827d0-a0b7-49d2-bf15-5c0b0610710c"
            # ("1187e6d0-c28d-4b11-b960-b14a9f28bb51",
            #  "e151340d-e7b4-43a3-bdd9-2f749babfc6b")
            # ("9897de75-ee4f-4677-89a3-6e6efd2ef6ae",
            #  "a7cc1161-cb41-4e5d-aa41-118a1f06cd12")
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "00b655a5-1def-49b6-9306-2374c1de23c5"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "01835f86-19fd-45bb-934c-459e87acc8f8"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "0e539687-2721-4e96-b637-f8736802344b"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "4cea65ff-044e-4292-bf7f-f2992bba6cd8"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "5bf3696c-6c64-4309-90b5-0c7d33b55222"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "5d363d2e-0791-4ecf-91bd-34cd8926d966"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "89f13e43-bf25-481c-bc14-98618be2c5f7"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "9a18957a-c129-4042-9f9f-33215f516549"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "a8ca8ddd-b249-4806-a17c-126f0fbe719a"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "a9846552-fb38-4f69-9d27-01c5b879d761"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "b7368ed6-379d-4e49-be60-a94ccc9e5702"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "badd97da-0c08-4706-9c38-8c5a16a6ba10"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "c13c6e69-7e2e-4421-beb1-2f49c4e0d994"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "dc248c0c-bde7-49b6-9778-e8724f332c6d"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "dc8c5185-4201-4532-91ad-f6e955df5dfb"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "e3d1694f-c4e4-49a0-b8d6-dae6c1a2b2a8"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "e971d3da-defb-4b0c-8c2d-46af09dbb7ad"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "eaec49f6-3f98-4098-a13c-52bbe3c1a1c3"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "ee2878ca-1395-4124-a61d-10b00df44698"],
            # ["3831255c-e5f3-4b3f-9ab5-6cc640530e40", "f1d45305-c977-46b0-807d-c11640dc1afa"]

            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","02d2b88d-45b3-419a-915a-a1b5b613a475"],   # Sam's club
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","02d2b88d-45b3-419a-915a-a1b5b613a475"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0b236c13-45a4-480e-8b0d-6f5031a35faa"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0b236c13-45a4-480e-8b0d-6f5031a35faa"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","1018b871-a085-468c-80bb-0db65c32aff3"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","1018b871-a085-468c-80bb-0db65c32aff3"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","17943bd8-be2a-433a-8683-48168cec7465"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","17943bd8-be2a-433a-8683-48168cec7465"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","1ea95fab-2721-418a-8bc2-5782074f46b5"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","1ea95fab-2721-418a-8bc2-5782074f46b5"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","22020457-6028-4e73-98a4-67bd6f5face7"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","22020457-6028-4e73-98a4-67bd6f5face7"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","2d485540-e472-4f02-b893-b86a94f3d581"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","2d485540-e472-4f02-b893-b86a94f3d581"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","377f888e-9ccc-442d-bb8a-f10a3721d57a"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","377f888e-9ccc-442d-bb8a-f10a3721d57a"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","39e6f3fd-4799-41b9-b5fa-31ea203d4e2b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","39e6f3fd-4799-41b9-b5fa-31ea203d4e2b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5079c1ce-7e3d-45fe-9c55-8f51fed2f51e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5079c1ce-7e3d-45fe-9c55-8f51fed2f51e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","509b27ea-34d9-42f1-a2c6-1eaef6b1763b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","509b27ea-34d9-42f1-a2c6-1eaef6b1763b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","54d83014-4cf7-4947-bc37-3679cb111162"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","54d83014-4cf7-4947-bc37-3679cb111162"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","54d83014-4cf7-4947-bc37-3679cb111162"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","54d83014-4cf7-4947-bc37-3679cb111162"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5c61a89e-13ba-4d89-bd6c-168cbd3d4e49"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5c61a89e-13ba-4d89-bd6c-168cbd3d4e49"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5d720e6d-e920-4828-8869-e9865e9310a3"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5e15efa1-593b-4122-ae62-d12f3426510e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5e15efa1-593b-4122-ae62-d12f3426510e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6b7038d4-458f-4445-b6ee-d0b8493eb6da"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6b7038d4-458f-4445-b6ee-d0b8493eb6da"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","73d13dae-6bda-4d8d-b75d-3ba9903998f3"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","73d13dae-6bda-4d8d-b75d-3ba9903998f3"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","773b8230-875e-46fe-a5d7-06defbdec459"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","773b8230-875e-46fe-a5d7-06defbdec459"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","7d63aabd-60bc-47ee-89d5-68d3c61af69f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","7d63aabd-60bc-47ee-89d5-68d3c61af69f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8cdaf3f4-9c5a-4005-97c4-2477dc239998"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8cdaf3f4-9c5a-4005-97c4-2477dc239998"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","91e0f665-2fa2-4d8e-8508-768a63024434"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","91e0f665-2fa2-4d8e-8508-768a63024434"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9441bfdb-4996-4975-a213-60a1b0b84837"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9441bfdb-4996-4975-a213-60a1b0b84837"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9aa173cb-9731-4a31-8d75-e69ad7c7ff2d"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9aa173cb-9731-4a31-8d75-e69ad7c7ff2d"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9e29c934-9686-42db-a69b-a635c010d383"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9e29c934-9686-42db-a69b-a635c010d383"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ac2e1998-3c5e-404d-a93a-58c24cb42393"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ac2e1998-3c5e-404d-a93a-58c24cb42393"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","b1399c3f-116c-4bcb-9d7a-ce6267bb57a9"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","b1399c3f-116c-4bcb-9d7a-ce6267bb57a9"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","c545bdda-bb4f-46aa-b663-56ce0748d996"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","c545bdda-bb4f-46aa-b663-56ce0748d996"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cc3fe23d-84cd-4f7f-9ef6-89cacb15c79b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cc3fe23d-84cd-4f7f-9ef6-89cacb15c79b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cc4afe16-58d7-4923-bee7-7afec370ee11"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cc4afe16-58d7-4923-bee7-7afec370ee11"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cead98ec-8fa6-40e8-81e7-3dd983505eeb"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cead98ec-8fa6-40e8-81e7-3dd983505eeb"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","d05876ad-6487-490f-aca0-d4a63077b676"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","d05876ad-6487-490f-aca0-d4a63077b676"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ddd9bf50-401a-40fb-bfc2-0dee95746727"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ddd9bf50-401a-40fb-bfc2-0dee95746727"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ed704546-a98c-4427-b8fd-02ca553664b0"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ed704546-a98c-4427-b8fd-02ca553664b0"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f0cf5c8d-38ce-41ab-8bd4-a3da9d98226f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f0cf5c8d-38ce-41ab-8bd4-a3da9d98226f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f5bf43f1-fc58-49db-ad0c-26f775820c06"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f5bf43f1-fc58-49db-ad0c-26f775820c06"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","fad8715f-6feb-40b2-a9b4-d1f0e5ac9b2f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","fad8715f-6feb-40b2-a9b4-d1f0e5ac9b2f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ff8d8ecd-bbfd-4ae3-84d4-0dbbf29d4ddc"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ff8d8ecd-bbfd-4ae3-84d4-0dbbf29d4ddc"]
        ]

        sites_2 = [
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","00272816-8dc2-4f13-8c94-56341bfa6542"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","014638db-4168-405f-a7db-218c4c303ee4"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","015670cc-6bdb-4d13-8a99-8b0b71735236"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","01a3e341-3b94-4fe3-bfdb-431a253e1cc3"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","02d2b88d-45b3-419a-915a-a1b5b613a475"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","04d1d367-9feb-4e30-b480-d95373d4e8d8"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","055fe640-9265-46b6-9cbd-7aeb6a45dd2f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","05842190-e40a-4614-a955-40a1c285fc27"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","05c2f2d3-fc05-47ee-a4ef-f87cedc5bf8f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","067a304c-faea-4d71-b2ff-d0a79970ea25"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","07be0f7e-2c9a-4b1f-998f-76e2860a75e3"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0903d0e8-d25c-4cc1-91b6-5d74d9951111"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","09917502-5b85-484f-b638-566523ff9b5e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","09bdb0d6-4231-47fb-9192-40227d46c778"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0a039436-4cae-4de4-bda8-dbc24f324e25"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0a313bd0-9628-46f4-a6d5-fa766b1bbd31"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0aae1670-ec04-4344-89a9-69c88ea34049"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0b236c13-45a4-480e-8b0d-6f5031a35faa"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0c96f798-9542-4a12-b3f7-dedf303d6b9e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0d9adea7-3678-4a53-8b18-c06b37f34fb0"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0dc24cf1-b75b-4fe2-8f0c-06fba7ed2c7c"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0e5ae4bb-e454-4cca-ba82-0056de691b11"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0ea4d88e-efd9-435c-960b-daa3bb6c5e52"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0ec6eeb2-11f8-440a-b24f-6fce273f89a0"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","0f6f1b85-a702-4853-9273-fa06efb4a737"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","1018b871-a085-468c-80bb-0db65c32aff3"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","110966f7-f82f-422e-9bf9-12d40c9b41eb"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","113dcfbd-661e-4950-a081-af3f71070092"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","151d6565-dfd7-415d-aec0-3a866015f33b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","15e10791-14a4-4085-9e03-9fca95eaa2d6"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","16ef31e8-9e8b-4f62-9809-8e484a1c4840"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","174b6f0a-b324-4c83-b52c-41bbc8d3f204"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","17943bd8-be2a-433a-8683-48168cec7465"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","182b1c8d-05fa-4f53-8af2-761825153468"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","194976d9-dac1-410f-90f8-5dd1a536037e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","1b09e83e-495e-4911-9b54-e3197ace36f4"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","1b156f9c-9440-4b83-84ce-6162a1d65201"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","1b2b3c41-a1b2-478b-a980-25b1a51908d2"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","1cdc6a74-3106-4ab1-88d5-86c1854ff71c"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","1df0cb1b-389d-4c60-b598-9cdff8566bd5"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","1ea95fab-2721-418a-8bc2-5782074f46b5"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","21f40e8c-6f42-48e0-bb2b-3b777a564d14"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","22020457-6028-4e73-98a4-67bd6f5face7"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","2840e319-07b5-4764-b6a2-2cdc01e83f28"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","28c21519-7340-42d7-b5fe-13ed1da9e012"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","2ac50e76-c747-4de5-bef7-a4f02bb4d42a"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","2b546d49-52af-4685-b83b-d2747482b35e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","2d0bf4a9-e458-4a68-bb9b-c324f9099165"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","2d14ee9d-c306-4992-b7b5-96a8bb100c4c"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","2d485540-e472-4f02-b893-b86a94f3d581"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","2d4a506a-195b-4f52-aa4c-0616c286a4bf"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","2d75f42b-b958-45c6-bc52-49fd60870942"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","301cb473-2559-4733-b410-692b843588a9"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","346e26f7-fc07-400f-8f02-9488e728fa71"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","377f888e-9ccc-442d-bb8a-f10a3721d57a"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","38fdd98c-6e7b-4e20-a4b3-fb6bcefeb083"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","39e6f3fd-4799-41b9-b5fa-31ea203d4e2b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","3a0c5f94-fc41-4ca0-b781-d95030c59316"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","3a0dbff5-4897-446b-a824-70441543f751"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","3c053e44-9b5a-4969-b34e-5df8e4868ee3"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","3e5abbb1-cd9b-4887-8fd3-cc1df490bb74"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","3f7f953c-cab5-4c54-9fbc-1eddd24db2da"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","3f93f63a-57c9-41b4-be5d-216dc6889a36"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","41e428f5-7db1-46e7-b1eb-b59554479b86"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","43aa803b-5a8c-46cc-ac13-495ca1fa2056"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","445802ff-7879-4b43-84c8-0e16335be678"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","447fe715-0c14-45eb-80c7-6d02c5bfd203"],

            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","46141d89-4066-43c0-914b-dda97152a473"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","46443cfe-91dd-4073-baa7-8517030b2b84"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","4784c517-eb9c-44fa-b0a5-1b9bdf3a6195"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","47ec85e0-6506-4dec-b018-b5dba1e3eee8"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","488d4f74-bcff-47c3-b9a3-c5b029a4bb82"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","49b64ac8-12fd-4ac4-8bc3-cd08838f8032"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","49e119f5-a33c-4302-a57f-c8672712ec33"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","4a6d1050-f6e5-4939-aeed-2c00b0a67664"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","4b4d42ff-a4b9-44c9-b9b5-56e5eb87b309"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","4b737807-ba00-489e-bd5b-fd113340cb49"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","4d4ce9a0-4482-457b-99bb-d18cb51b973e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","4ea1db22-8a68-4eab-b38c-292d4257d2a5"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","4f766499-185d-42e7-ac7d-4363d2995f14"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","4faf5763-38ba-4645-9657-0e1570e51cd8"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5079c1ce-7e3d-45fe-9c55-8f51fed2f51e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","509b27ea-34d9-42f1-a2c6-1eaef6b1763b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","52200fb0-9fa7-4415-a204-98fcabd62295"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","54385bce-6a77-4486-8866-a91de9bac6f1"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","54d83014-4cf7-4947-bc37-3679cb111162"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","56f5539f-3993-4ea6-82e7-77ea0022f21e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","57a0d421-bad7-433e-82fc-b337bfc70dd3"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5918266c-5e96-43b0-b992-65832b986fab"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","591f6257-bf34-4d70-9722-3176358dac9e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","596606b7-1db8-4e51-8cc9-6b997374c368"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5ab67e40-153e-4496-adb3-4aae514639ee"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5ac19a52-f670-4d0c-85d8-23fda19e2584"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5b37a9d1-d46f-4231-81de-f28cc2b2a642"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5b98b2cf-0e0a-4018-9e80-12c26199bee8"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5bc8fe9c-e1f2-46b0-b692-42ac5bf270ec"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5be4793a-cf74-4ac5-90b0-aa49a12c089c"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5bf82f98-963d-4966-b1ac-37ec9d5d5a45"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5c61a89e-13ba-4d89-bd6c-168cbd3d4e49"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5ced810d-0d35-4bbc-b5d0-2d7417b9dbe0"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5d720e6d-e920-4828-8869-e9865e9310a3"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5df70d66-686c-4c10-8160-f4f474d9cb3c"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5dfe6d04-5b39-4caa-865e-6253888cc7e1"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","5e15efa1-593b-4122-ae62-d12f3426510e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","60965e04-4ce6-47e7-8902-f8d88c3e482a"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6164cd85-6cb2-4141-8323-5a650b543545"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6194ca0f-64a0-4eb6-bb6b-beee4d72f29a"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","61ad21e5-09f3-4825-be12-e34bfe18b2da"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","63550e79-4ac0-4775-b314-3a8b4e320e9b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","637c860f-1adf-41cf-8184-2930584e533d"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","641fc6bc-c098-4eb8-b60c-81628f5360a6"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","64737839-b6c5-45b0-bae5-35b059beb9e5"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","65e04995-de71-4863-b3f2-78707e180842"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6785ab02-2a3c-4eb6-9814-958acc224ed5"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","682e99a3-eb66-4fb0-9335-3845bc54f092"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","68a6e703-cdee-47ff-b361-5e3a54cb69b2"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","68eb5ac3-4f59-4a29-9623-0e680b4daed0"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","696d34dc-98f1-4e15-9837-38fbf19b7f1e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6a418f8f-540a-4866-b5f8-f9e495675048"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6a6dd652-ac3e-4b2b-9866-a7f8973a4009"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6a8a5354-0e22-4bce-9a76-71aab436a091"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6ae8d819-25d9-4fce-8cd5-a75b6300b2f2"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6b7038d4-458f-4445-b6ee-d0b8493eb6da"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6c7657a9-8dc5-4128-a585-b08b4dcb70c1"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6cd35e79-3033-43a5-a0b4-aa15abf2cc0c"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6d4c3bb5-d6d0-4aa5-a73e-4102367dd6bf"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6e38d887-350e-4790-809b-cbb5a3ea1122"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6eb59d1f-a52d-4922-af96-67243e75072e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","6f90f2fa-2417-4203-9e98-27e62f24778d"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","705c9f38-babd-48fd-9881-f4a9b6ff3516"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","71423a3c-a327-4369-bbec-005942491c30"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","71ffaeeb-c62c-4c78-9d9f-1d66038eac84"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","73d13dae-6bda-4d8d-b75d-3ba9903998f3"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","756f8b87-703b-4ab7-a0eb-420efaa1dcd6"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","773b8230-875e-46fe-a5d7-06defbdec459"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","77e3151b-ccb6-429a-8aa4-f89aa2e8a5aa"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","78f8dcfd-6f7d-4f7e-b663-4eb03f37261c"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","7b44bf58-0790-4a62-8a2e-d9d035ac6e28"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","7cf592ec-61f3-414c-a47e-960b39785ff2"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","7d63aabd-60bc-47ee-89d5-68d3c61af69f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","82f66599-0688-47d4-9a6f-2b1440805112"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","88c8ccd6-46f3-4e59-a35e-327f96050236"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8b7ae1a6-28b6-40f5-a709-9879f5bacfff"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8bec9c24-dc5d-42fe-aad5-d2eff1044409"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8cdaf3f4-9c5a-4005-97c4-2477dc239998"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8d2792d6-9b3e-46d1-a65c-d8aeffdb6a62"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8d5c8acb-9de9-4ce2-bacb-909dd31f2f57"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8d93f050-b36a-49e5-9ac5-3ff9a46f8822"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8d9d8e81-d145-42e5-8f0a-21141b61e49b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8da27b02-6a65-47d7-aedb-a6ece57c54ba"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8e2d3bd8-9efe-41d8-b627-a9459b245509"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8fa330f8-7864-4c7f-964a-126646c6419f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","8ff32445-71b3-4053-b02b-6fe40edc7f4c"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","90e2404d-a466-4398-9956-6b039d11342a"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","91e0f665-2fa2-4d8e-8508-768a63024434"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9441bfdb-4996-4975-a213-60a1b0b84837"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","98f9558d-1a2d-4576-a99d-8f0857d4cce7"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9a0a4286-37d3-40e6-aa94-61d86b65cedf"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9a1a473b-ced5-4938-b211-be18a721477f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9a3affde-f69d-4085-868a-0d41b20f81c4"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9aa173cb-9731-4a31-8d75-e69ad7c7ff2d"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9b7397e2-3c4d-4b9c-9896-37c3e7e51b49"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9cf2516c-b3a7-4910-b572-93de483d7f6c"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","9e29c934-9686-42db-a69b-a635c010d383"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","a115ee78-d1af-4af2-b290-8c44bd37aa25"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","a18a2ef5-e266-4b5a-b2ce-7a5822deb9de"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","a2e1f66a-d30c-48a0-921c-6a3c58473243"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","a2fd4f26-25d1-478b-bfb8-55b848389ffb"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","a38a6657-c4b6-40fc-a248-0e068fe60693"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","a7092875-257f-43f3-9514-ca1ab688bec0"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","a8974105-e835-4f0d-a292-0e688d2760cb"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","aa42c985-0555-45a2-af6b-634f5fbb7142"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","aadda21f-ac6a-41de-9a68-55bc8204757b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","abd9ecba-06e4-41d1-a91c-d9ee31438726"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ac0f6ff2-14e2-4781-82e7-2d52a732e074"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ac2e1998-3c5e-404d-a93a-58c24cb42393"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","acc19b30-96fa-4515-b931-254bafbcab32"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","acddcc1a-0637-4845-a7f4-cadf1b3f5e79"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","adff1b74-fa76-435a-b254-9cb41e584da4"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","b1399c3f-116c-4bcb-9d7a-ce6267bb57a9"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","b47053cc-1558-45fd-b45d-3af2442e13cc"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","b5ae8988-e1b4-49d7-ae91-c4b9249950f1"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","b5dd2a9d-2498-485e-8ed8-df5bd0a4c28e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ba699962-a16b-476f-a8cd-b83b66d05108"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","bb503480-581c-430f-9fcf-38497ff0a97c"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","bbcfd08e-bd9d-4d68-9a01-f6fc8dffd090"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","bc3e8431-1683-4ba7-8be4-0af99afe32d4"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","bd335f5d-ec1c-4327-af79-da020e2522c4"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","bfa9c073-afb4-4342-8790-b2937ba8801d"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","c07b98db-c06b-4d0b-99e3-81025a267d01"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","c154abbe-cc9a-4bf7-b8e0-cd836fd2f507"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","c1d3e838-28e6-4cf9-8c9d-0f5c022dcc12"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","c26f3e41-3bc8-470d-8aef-372827e7e2b6"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","c30096d0-1a99-41fa-947e-090518054869"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","c416af3b-a16c-4b5d-85c9-fc46a62ce7a7"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","c545bdda-bb4f-46aa-b663-56ce0748d996"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","c7a957d2-53ea-474e-8ff2-9cb42d9d33e8"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","caf364bf-c95e-4859-8864-9494785225bf"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cc2767ec-e6df-4304-b7fd-ed5c8e32dfa8"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cc38e79e-7294-4fdd-b020-5ace18a2fc6e"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cc3fe23d-84cd-4f7f-9ef6-89cacb15c79b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cc4afe16-58d7-4923-bee7-7afec370ee11"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cd18b68c-cf79-4d6e-9f7d-3289d3e2bf53"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cd1cc26d-c581-40aa-8340-cba7def69294"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cead98ec-8fa6-40e8-81e7-3dd983505eeb"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cf2249f3-a945-41cc-9458-1324ff2e452b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","cf768769-dfec-4e5c-bb5d-712793a24b46"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","d05876ad-6487-490f-aca0-d4a63077b676"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","d1d418d6-8621-429b-a9f5-1dae7fbe9e01"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","d2039f13-7a74-48f3-b465-ddfd969289f1"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","d56cbe9e-0d41-4047-ad12-a22fe05da4d9"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","d6e5dccd-da7a-457a-9b36-79701a9bdd6c"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","d858dd53-3ccf-458e-a479-ff8f2b260793"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","d97f05ee-6a6c-4210-841b-57e84f72d816"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ddd9bf50-401a-40fb-bfc2-0dee95746727"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","dfb1bf10-20d8-4737-a8ec-eae8a517c2c6"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","e0188891-1368-431c-8729-58592cf23349"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","e07dde37-ca46-478c-b494-094d6d744f9c"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","e5d06f5c-7ee7-48c9-aa37-8dbecfa3f551"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","e6754430-5afd-45f3-acb4-e2d69fe06df4"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","e9e218b7-f0f8-49e0-aa67-acb65e8cc6c5"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","eaa1c1e0-b825-4c5a-a548-e05fb7d9d854"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","eb0bb50d-71d9-45b7-babe-2c98a633d8ea"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ed4747d0-df77-4db1-8775-82f43e1f97a0"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ed704546-a98c-4427-b8fd-02ca553664b0"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f0cf5c8d-38ce-41ab-8bd4-a3da9d98226f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f1b639b5-b493-4b01-9a3a-25400ca02fac"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f3bf9745-3b57-4774-b897-48b622dfb9be"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f3efe691-ac35-4817-ba9b-7b52f0a99567"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f4904b25-07f6-4f16-bfc9-eb08bf249274"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f4a753c2-a7be-4839-abe3-fe884364b360"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f4dc154d-dc2b-4ccc-9264-c06a6c4edbc2"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f5a7a862-b711-4bad-9b3d-b8a641af072b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f5ab823f-f4e5-4983-a274-8cb5d281325f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f5bf43f1-fc58-49db-ad0c-26f775820c06"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f6d3aaa9-6303-48ac-9527-b30b61173a5a"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f7c3293c-e67f-4620-971f-8ac78fbe4b20"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","f932fff7-85e1-4875-b859-c00c73bef5e2"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","fa4e90e0-5172-43c6-aa59-9a0bc324790b"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","fad8715f-6feb-40b2-a9b4-d1f0e5ac9b2f"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","fb34b99d-beb2-4ec2-ba4e-61fbe41ba526"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","fd3ac764-9f70-4313-a844-f9249f37543d"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","fdc13857-1c3b-442c-8998-6dc9979d18df"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","fe5d7fbc-9590-4532-bf31-cc1645127370"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","fedb3853-8c64-4a8b-9094-6bdaed73d2bb"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","fedb850f-8146-4339-960b-2bc791f5c3e1"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ff169c83-1ff4-48ce-a79c-7e5d9dc0c2ad"],
            ["604411f1-4e45-4bed-9a69-cc37b247fdf9","ff8d8ecd-bbfd-4ae3-84d4-0dbbf29d4ddc"],

        ]

    n = 0
    n0 = 0
    for org_site in sites_2:
        n0 += 1
        if org_site in sites:
            pass
        else:
            n +=1
            org, site = org_site
            msg = get_cmd_global(site, ["24"], org)
            print(msg)
            publish_rrm_global(json.loads(json.dumps(msg)))
            time.sleep(1)

    print(n0, n)

    # for site in sites[:]:
    #     # msg = get_cmd(siteband[0], siteband[1])
    #     org, site = site
    #     msg = get_cmd_global(site, ["24"], org)
    #     print(msg)
    #     publish_rrm_global(json.loads(json.dumps(msg)))
    #     time.sleep(1)
    # publish_acs("")
    # time.sleep(30)


# def test_acs():
#     site = "d664e3a5-8fc1-43c7-bfba-f3236ab02746"
#     site = "f4d40c28-4aa3-11e5-8510-1258369c38a9"
#     ap = "5c-5b-35-0e-00-38"
#
#     ap= "5c-5b-35-0e-61-68"
#     site = "cd5a3b5a-6b21-11e6-a17e-0242ac110005"
#     org = '1187e6d0-c28d-4b11-b960-b14a9f28bb51'
#     site = 'c8a827d0-a0b7-49d2-bf15-5c0b0610710c'
#     ap = '5c-5b-35-3e-af-dc'
#     # ap = '5c-5b-35-3e-af-e6'
#     # ap = '5c-5b-35-3e-ac-df'
#     publish_rrm_acs(org, site, ap, '2.4')


if __name__ == "__main__":
    # for i in range(1):
    #     test_acs()
    #     time.sleep(1)
    # publish_mes()
    test_global()
    # publish_rrm_apscan()
    # publish_sle_capacity_anomaly()

    # publish_acs_dfs()
    #
    # data = {
    #     "ap_id": "5c-5b-35-0e-bd-03",
    #     "band": "24",
    #     "command": "acs-request-ap",
    #     "org_id": "f4d3653e-4aa3-11e5-8510-1258369c38a9",
    #     "site_id": "29d2648e-e66b-11e5-a6cd-0242ac110003",
    #     "timestamp": 1560895818.128067
    # }
    #
    # publish_rrm_acs(data.get('org_id'), data.get('site_id'), data.get('ap_id'), data.get('band'))

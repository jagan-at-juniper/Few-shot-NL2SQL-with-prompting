org_id = '9c3e516c-397d-11e6-ae35-0242ac110008'
rdd_t = rdd_mobile_client.map(lambda x: json.loads(x[1])) \
    .filter(lambda x: x.get("InfoFromTerminator").get("OrgID") == org_id ) \
    .map(lambda x: {"MAC": x["Result"]["ClientInformation"]["Device"]["MAC"],
                    "Model": x["Result"]["ClientInformation"]["Device"]["Model"],
                    "HostRaw":  x["Result"]["ClientInformation"]["Device"]["HostRaw"],
                    "Timestamp":x["Result"]["Stats"]["Passive"]["PassiveTimestamp"],
                    "Tests": x["Result"]["Stats"]["Passive"]["Tests"]
                    }
         )

# def flat_scan(x):
#     res=[]
#     mac = x.get("MAC", "")
#     model = x.get("Model")
#     ts = x.get("Timestamp")
#     for test in x.get("Tests"):
#         WiFiConnectionData = test.get("WiFiConnectionData")
#         ConnectionType = x.get("ConnectionType", "")
#         wifi_rssi = WiFiConnectionData.get("RSSI")
#         wifi_bssid = WiFiConnectionData.get("BSSID")
#         ip = WiFiConnectionData.get("IP")

#         for scan in test.get("WiFiScanData"):
#             scan["MAC"] = mac
#             scan["model"] = model
#             scan['ip'] = ip
#             scan["type"] = ConnectionType
#             scan["ts"] = ts
#             scan["wifi_rssi"] = wifi_rssi
#             scan["wifi_bssid"] = wifi_bssid

#             res.append(scan)
#     return res

"""
'WiFiConnectionData': {'BSSID': '5c:5b:35:20:d8:c1',
                                                                    'Frequency': 2412,
                                                                    'IP': '192.168.10.146',
                                                                    'LinkSpeed': 72,
                                                                    'MaxSupportedRx': -1,
                                                                    'MaxSupportedTx': -1,
                                                                    'NetworkId': 0,
                                                                    'RSSI': -57,
                                                                    'RxLinkSpeed': -1,
                                                                    'SSID': 'banana',
                                                                    'SignalStrength': 94,
                                                                    'SupplicantState': 'COMPLETED',
                                                                    'TxLinkSpeed': -1,
                                                                    'WifiStandard': 'IncompatibleWithAndroidVersio"""
def flat_wifi_connection(x):
    res=[]
    mac = x.get("MAC", "")
    model = x.get("Model")
    ip = x.get("HostRaw", [None, None, None ])[1]
    ts = x.get("Timestamp")
    #     ts = x.get("Timestamp")
    for test in x.get("Tests"):
        if test:
            WiFiConnectionData = test.get("WiFiConnectionData", [])
            WiFiConnectionData["MAC"] = mac
            WiFiConnectionData["model"] = model
            WiFiConnectionData["ts"] = ts

            res.append(WiFiConnectionData)

    return res
# rdd_t.flatMap(lambda x: [xx.get("WiFiScanData") for xx in x.get("Tests")]).first()

# df_scan = rdd_t.flatMap(lambda x: flat_scan(x)).toDF()
# df_scan.printSchema()


# df_wifi = rdd_t.flatMap(lambda x: flat_wifi_connection(x)).toDF()
# df_wifi.printSchema()

rdd_t.flatMap(lambda x: flat_wifi_connection(x)).first()
# rdd_t.first()

from pyspark.shell import spark

env = "production"
# env = "staging"


# s3_bucket = "s3://mist-aggregated-stats-{env}/entity_event/entity_event-{env}/".format(env=env)
date_day = "2020-03-1*"
hr = '*'

# s3_capacity_path = "dt={day}/hr={hr}/CapacityAnomalyEvent_*.seq".format(day=date_day, hr=hr)
# s3_coverage_path = "dt={day}/hr={hr}/CoverageAnomalyEvent_*.seq".format(day=date_day, hr=hr)
# s3_capacity_path = s3_bucket + s3_capacity_path
# s3_coverage_path = s3_bucket + s3_coverage_path
# print(s3_capacity_path, "\n", s3_coverage_path)

s3_bucket = "s3://mist-secorapp-staging/sle-capacity-anomaly/sle-capacity-anomaly-staging/".format(env=env)
s3_path = s3_bucket + "dt={day}/hr={hr}/*.seq".format(day=date_day, hr=hr)

print(s3_path)
rdd_capacity = spark.sparkContext.sequenceFile(s3_path)

rdd_capacity.first()


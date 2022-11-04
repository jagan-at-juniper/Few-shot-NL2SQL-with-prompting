import os
from datetime import datetime, timedelta
import numpy as np

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler

env = "production"
provider = os.environ.get("CLOUD_PROVIDER", "aws")
provider = "aws"
# provider = "gcp"
fs = "gs" if provider == "gcp" else "s3"

app_name = "es-client-events"
spark = SparkSession \
    .builder \
    .appName(app_name) \
    .getOrCreate()
spark.sparkContext.setLogLevel("warn")


def flatt_df(df, target_col="ev_type", event_types=[], groupby_cols=[]):
    """
    Flat dataframe, preparation for vectorization
    :param df:
    :param cols:
    :return:
    """
    df_flatten = df.groupBy(groupby_cols).agg(
        *(F.sum(F.when(F.col(target_col) == c, 1).otherwise(0)).alias("count_" + c) for c in event_types if c),
        F.sum(F.when(F.col(target_col) == None, 1).otherwise(0)).alias("count_none")
    )
    return df_flatten


@F.udf
def sim_cos(v1, v2):
    """
     Cosine Similarity of Vector1 and Vector2
    :param v1:
    :param v2:
    :return:
    """
    try:
        p = 2
        return float(v1.dot(v2)) / float(v1.norm(p) * v2.norm(p))
    except:
        return 0


def get_cosine_similarity(df1, df2):
    """

    :param df1:
    :param df2:
    :return:
    """
    result = df1.alias("i").join(df2.alias("j")) \
        .select(sim_cos("i.features", "j.features").alias("sim_cosine"))
    return result


@F.udf
def max_index(a_col):
    if not a_col:
        return a_col
    if isinstance(a_col, SparseVector):
        a_col = DenseVector(a_col)
    a_col = vector_to_array(a_col)
    return np.argmax(a_col)




def fetch_data(date_day, date_hour, fs="s3", env="production"):
    """

    :param date_day:
    :param date_hour:
    :param fs:
    :return:
    """
    s3_path = f"{fs}://mist-secorapp-{env}/es-client-event/es-client-event-{env}/dt={date_day}/hr={date_hour}"
    print(s3_path)

    # df = spark.read.parquet(s3_path)
    df = spark.read.format('orc').load(s3_path)
    # df.printSchema()
    return df


def get_feature_list(df, cols=['ev_type']):
    """

    :param cols:
    :return:
    """
    # cols = ['ev_type']
    # df_mist = df.groupBy(cols).agg(F.sum(F.lit(1)).alias("count"),
    #                                F.countDistinct("ap").alias("impacted_aps"),
    #                                F.countDistinct("wcid").alias("impacted_client"))
    # df_mist.show(100, truncate=False)
    #
    # event_typs = list(df_mist.select('ev_type').toPandas()['ev_type']) #  list of event_typs
    # print(event_typs)
    # event_typs = list(df_mist.select('ev_type').toPandas()['ev_type']) #  list of event_typs
    # print(event_typs, len(event_typs))

    event_typs = ['MARVIS_EVENT_CLIENT_FBT_FAILURE', None,
                  'CLIENT_AUTH_ASSOCIATION_OKC',
                  'CLIENT_DNS_OK', 'CLIENT_AUTHENTICATED_11R',
                  'CLIENT_REASSOCIATION_PMKC', 'STA_DOS_DISASSOC',
                  'MARVIS_EVENT_CAPTIVE_PORTAL_REDIRECT',
                  'CLIENT_DEAUTHENTICATION', 'CLIENT_GW_ARP_FAILURE',
                  'STA_EAPOL_START_STORM', 'CLIENT_REASSOCIATION',
                  'CLIENT_ARP_FAILURE',
                  'MARVIS_EVENT_CLIENT_AUTH_FAILURE_11R', 'STA_EAP_HANDSHAKE_FLOOD',
                  'MARVIS_EVENT_CLIENT_MAC_AUTH_FAILURE',
                  'MARVIS_EVENT_WXLAN_CAPTIVE_PORT_FLOW_REDIRECT',
                  'CLIENT_AUTH_ASSOCIATION', 'MARVIS_EVENT_CLIENT_DHCP_STUCK',
                  'MARVIS_EVENT_CAPTIVE_PORTAL_FAILURE',
                  'MARVIS_EVENT_CLIENT_STATIC_IP_BLOCKED',
                  'MARVIS_EVENT_CLIENT_DHCP_FAILURE',
                  'MARVIS_EVENT_CLIENT_AUTH_FAILURE_OKC',
                  'CLIENT_AUTHENTICATED', 'CLIENT_AUTH_REASSOCIATION_OKC',
                  'CLIENT_AUTH_REASSOCIATION_11R', 'MARVIS_EVENT_CLIENT_DHCP_NAK',
                  'DEFAULT_GATEWAY_SPOOFING_DETECTED', 'CLIENT_DEASSOCIATION',
                  'MARVIS_EVENT_CLIENT_DHCPV6_STUCK', 'CLIENT_ASSOCIATION_FAILURE',
                  'MARVIS_EVENT_CLIENT_DHCPV6_FAILURE', 'MARVIS_EVENT_CLIENT_STATIC_DNS_BLOCKED',
                  'MARVIS_EVENT_SAE_AUTH_FAILURE', 'SA_QUERY_TIMEOUT',
                  'MARVIS_EVENT_CAPTIVE_PORTAL_AUTHORIZED', 'CLIENT_GW_ARP_OK',
                  'CLIENT_IP_ASSIGNED', 'MARVIS_EVENT_CLIENT_WXLAN_POLICY_LOOKUP_FAILURE',
                  'CLIENT_IPV6_ASSIGNED', 'MARVIS_DNS_FAILURE', 'MARVIS_EVENT_CLIENT_DHCPV6_NAK',
                  'REPEATED_AUTH_FAILURES', 'CLIENT_DEAUTHENTICATED', 'MARVIS_EVENT_CLIENT_AUTH_DENIED',
                  'CLIENT_ASSOCIATION', 'MARVIS_EVENT_WLC_FT_KEY_NOT_FOUND',
                  'MARVIS_EVENT_CLIENT_FAILED_DHCP_INFORM', 'HTTP_REDIR_PROCESSED',
                  'RADIUS_DAS_NOTIFY', 'CLIENT_LOCAL_SUPPORT_PAGE',
                  'CLIENT_AUTH_REASSOCIATION', 'CLIENT_EXCESSIVE_ARPING_GW',
                  'MARVIS_EVENT_CLIENT_AUTH_FAILURE', 'MARVIS_EVENT_STA_LEAVING'
                  ]
    return event_typs


def get_vectorized_df(df_mist_g, validating_features):
    """

    :param df_mist_g:
    :return:
    """
    vecAssembler = VectorAssembler(inputCols=validating_features, outputCol="features")
    vec_mist_g = vecAssembler.transform(df_mist_g)

    return vec_mist_g


def save_df_to_fs(df, date_day, date_hour, app_name="aps-no-client-all", band=""):
    """

    :param df:
    :param date_day:
    :param date_hour:
    :param app_name:
    :param band:
    :return:
    """
    date_hour = "000" if date_hour == "*" else date_hour
    s3_path = "{fs}://mist-data-science-dev/wenfeng/{repo_name}_{band}/dt={dt}/hr={hr}" \
        .format(fs=fs, repo_name=app_name, band=band, dt=date_day.replace("[", "").replace("]", ""), hr=date_hour)
    print(s3_path)
    # df.coalesce(1).write.save(s3_path,format='parquet',   mode='overwrite', header=True)
    df.write.save(s3_path, format='parquet', mode='overwrite', header=True)
    print(s3_path)
    return s3_path

#
# datetime_str = '2022-10-20 23:00:00'
# detect_time = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')

# screening_time = datetime.now() - timedelta(hours=4)
# date_day = screening_time.strftime("%Y-%m-%d")
# date_hour = screening_time.strftime("%H")
#
# detect_time_ref = screening_time - timedelta(days=1)
# date_day_ref = detect_time_ref.strftime("%Y-%m-%d")
# date_hour_ref = detect_time_ref.strftime("%H")

event_types = get_feature_list(None, [])
validating_features = ["count_" + c if c != None else 'count_none' for c in event_types]
print(validating_features)

# comparing
cosine_list = []
for delta_hr in range(0, 24):
    screening_time = datetime.now() - timedelta(hours=delta_hr)
    date_day = screening_time.strftime("%Y-%m-%d")
    date_hour = screening_time.strftime("%H")

    # reference/baseline
    ref_time = screening_time - timedelta(days=1)
    date_day_ref = ref_time.strftime("%Y-%m-%d")
    date_hour_ref = ref_time.strftime("%H")
    print("screening", screening_time, ref_time)
    # screening data
    df_screening = fetch_data(date_day, date_hour, fs, env)
    num_screening = df_screening.count()
    print(num_screening)
    if num_screening <1:
        break
    df_screening_flatten = flatt_df(df_screening, "ev_type", event_types)
    df_screening_vector = get_vectorized_df(df_screening_flatten, validating_features)

    # n_ref
    df_ref = fetch_data(date_day_ref, date_hour_ref, fs, env)
    num_reference = df_ref.count()
    print(num_reference)
    if num_reference <1:
        break
    df_ref_flatten = flatt_df(df_ref, "ev_type", event_types)
    df_ref_vector = get_vectorized_df(df_ref_flatten, validating_features)

    result = get_cosine_similarity(df_screening_vector, df_ref_vector)
    result.show()
    similarity = list(result.select("sim_cosine").toPandas()['sim_cosine'])[0]
    cosine_list.append([screening_time, ref_time, num_screening, num_reference, similarity])

    print("screening_time:{}:{}".format(date_day, date_hour), "ref={}:{}".format(date_day_ref, date_hour_ref),
          num_screening, "vs", num_reference, "sim_cosine", similarity)

# save result
similarity_Columns = ["screening_time", "ref_time", "num_screening", "num_reference", "similarity"]
df_similarity = spark.createDataFrame(data=cosine_list, schema = similarity_Columns)
df_similarity.printSchema()
df_similarity.show(truncate=False)

save_df_to_fs(df_similarity, date_day, "00", app_name, "")

print("Done!")
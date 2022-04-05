import datetime

import time

from cassandra.cluster import Cluster


def readFromCassandra( org, site, band, keyspace="sle_throughput_c", table="trend_by_site_10m"):
    """

    :param session:
    :param table:
    :param org:
    :param site:
    :param band:
    :return:
    """

    # keyspace = "rrm"

    from cassandra.cluster import Cluster

    cassandra_host = ["cassandra-throughput-000-production.mist.pvt"]
    cluster = Cluster(cassandra_host,
                      protocol_version=4,
                      port=9042,
                      cql_version='3.4.4',
                      connect_timeout=50.0
                      )

    keyspace = "sle_throughput_c"
    table = "sle_throughput_c.trend_by_site_10m"
    session = cluster.connect(keyspace)
    query = "select * from {table};".format(table=table)
    rows = session.execute(query)

    for row in rows[:10]:
        print(row)


    # query = "select * from {table} where org=\'{org}\' and site=\'{site}\' and" \
    #         " band=\'{band}\' and when>\'{start}\' and when<\'{end}\';" \
    #         "".format(table=table, org=org, site=site, band=band, start=start, end=end)

    # rows = session.execute(query)
    return rows
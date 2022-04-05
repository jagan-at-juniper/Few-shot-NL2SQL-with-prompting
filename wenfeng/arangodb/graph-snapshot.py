
#
# ./dev-scripts/okta_spark jupyter-notebook --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11
#
# spark_ipython  --name wenfeng --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11
#

from graphframes import GraphFrame
from graphframes.lib import *
from graphframes.lib import AggregateMessages as AM
from pyspark.sql.types import *
import pyspark.sql.functions as F

import time
def find_cycles(gx, max_iter=10):
    # create emtpy dataframe to collect all cycles
    cycles = sqlContext.createDataFrame(sc.emptyRDD(),
                                        StructType([StructField("cycle",ArrayType(StringType()),
                                                                True)]))

    # initialize the messege column with own source id
    # iterate until max_iter
    # max iter is used in case that the3 break condition is never reached during this time
    # defaul value=100.000
    for iter_ in range(max_iter):

        # message that should be send to destination for aggregation
        msgToDst = AM.src["message"]
        # aggregate all messages that where received into a python set (drops duplicate edges)
        agg = gx.aggregateMessages(
            F.collect_set(AM.msg).alias("aggMess"),
            sendToSrc=None,
            sendToDst=msgToDst)

        # BREAK condition: if no more messages are received all cycles where found
        # and we can quit the loop
        if(len(agg.take(1))==0):
            #print("THE END: All cycles found in " + str(iter_) + " iterations")
            break

        # apply the alorithm logic
        # filter for cycles that should be reported as found
        # compose new message to be send for next iteration
        # _column name stands for temporary columns that are only used in the algo and then dropped again
        checkVerties=(
            agg
                # flatten the aggregated message from [[2]] to [] in order to have proper 1D arrays
                .withColumn("_flatten1",F.explode(F.col("aggMess")))
                # take first element of the array
                .withColumn("_first_element_agg",F.element_at(F.col("_flatten1"), 1))
                # take minimum element of th array
                .withColumn("_min_agg",F.array_min(F.col("_flatten1")))
                # check if it is a cycle
                # it is cycle when v1 = v and min{v1,v2,...,vk} = v
                .withColumn("_is_cycle",F.when(
                (F.col("id")==F.col("_first_element_agg")) &
                (F.col("id")==F.col("_min_agg")),True)
                            .otherwise(False)
                            )
                # pick cycle that should be reported=append to cylce list
                .withColumn("_cycle_to_report",F.when(F.col("_is_cycle")==True,F.col("_flatten1")).otherwise(None))
                # sort array to have duplicates the same
                .withColumn("_cycle_to_report",F.sort_array("_cycle_to_report"))
                # create column where first array is removed to check if the current vertices is part of v=(v2,...vk)
                .withColumn("_slice",F.array_except(F.col("_flatten1"), F.array(F.element_at(F.col("_flatten1"), 1))))
                # check if vertices is part of the slice and set True/False column
                .withColumn("_is_cycle2",F.lit(F.size(F.array_except(F.array(F.col("id")), F.col("_slice"))) == 0))
        )

        #print("checked Vertices")
        #checkVerties.show(truncate=False)
        # append found cycles to result dataframe via union
        cycles=(
            # take existing cycles dataframe
            cycles
                .union(
                # union=append all cyles that are in the current reporting column
                checkVerties
                    .where(F.col("_cycle_to_report").isNotNull())
                    .select("_cycle_to_report")
            )
        )

        # create list of new messages that will be send in the next iteration to the vertices
        newVertices=(
            checkVerties
                # append current vertex id on position 1
                .withColumn("message",F.concat(
                F.coalesce(F.col("_flatten1"), F.array()),
                F.coalesce(F.array(F.col("id")), F.array())
            ))
                # only send where it is no cycle duplicate
                .where(F.col("_is_cycle2")==False)
                .select("id","message")
        )

        print("vertics to send forward iter=", iter_)
        # newVertices.sort("id").show(truncate=False)

        # cache new vertices using workaround for SPARK-1334
        cachedNewVertices = AM.getCachedDataFrame(newVertices)

        # update graphframe object for next round
        gx = GraphFrame(cachedNewVertices, gx.edges)

    # materialize results and get number of found cycles
    #cycles_count=cycles.persist().count()

    _cycle_statistics=(
        cycles
            .withColumn("cycle_length",F.size(F.col("cycle")))
            .agg(F.count(F.col("cycle")),F.max(F.col("cycle_length")),F.min(F.col("cycle_length")))
    ).collect()

    cycle_statistics={"count":_cycle_statistics[0]["count(cycle)"],
                      "max":_cycle_statistics[0]["max(cycle_length)"],
                      "min":_cycle_statistics[0]["min(cycle_length)"]}
    # end_time =time.time()
    return cycles, cycle_statistics


def graph_prep(dt="2021-02-26", hr="03"):

    s3_device_nodes_bucket = "s3://mist-aggregated-stats-production/" \
                             "aggregated-stats/graph/snapshots/" \
                             "device-nodes/dt={}/hr={}/".format(dt, hr)
    device_nodes_df = spark.read.parquet(s3_device_nodes_bucket)
    device_nodes_df= (device_nodes_df \
                      .withColumn("message",F.array(F.col("id")))
                      )

    s3_device_edges_bucket = "s3://mist-aggregated-stats-production/" \
                             "aggregated-stats/graph/snapshots/" \
                             "device-edges/dt={}/hr={}/".format(dt, hr)
    device_edges_df = spark.read.parquet(s3_device_edges_bucket) \
        .selectExpr("from as src", "to as dst", "*")

    g = GraphFrame(device_nodes_df, device_edges_df)

    return g


def test_circle_device_graph():
    g = graph_prep("2021-02-26", "03")

    cycles, cycle_statistics = find_cycles(g, 10)
    cycles.show()
    cycle_statistics


def test_bidirection_loop(dt="2021-03-18", hr="06"):

    import pyspark.sql.functions as F

    s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/graph/snapshots/"
    s3_device_edges_bucket = "{}device-edges/dt={}/hr={}/".format(s3_bucket, dt, hr)
    device_edges_df = spark.read.parquet(s3_device_edges_bucket)

    device_edges_df_1 = device_edges_df.select(F.col("source").alias("source1"),
                                               F.col("sourcePort").alias("sourcePort1"),
                                               F.col("target").alias("target1"),
                                               F.col("targetPort").alias("targetPort1"))

    device_edges_df_join = device_edges_df.join(device_edges_df_1,
                                                [device_edges_df.target==device_edges_df_1.source1,
                                                 device_edges_df.targetPort==device_edges_df_1.sourcePort1
                                                 ],
                                                how="inner")

    # check bi-directional edges
    device_bidirectional_df = device_edges_df_join.filter("source == target1 and sourcePort == targetPort1")

    # device_bidirectional_df.count()
    return device_bidirectional_df

def test_bidirection():
    import pyspark.sql.functions as F
    dt="2021-03-25"; hr="10"

    s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/graph/snapshots/"
    s3_device_nodes_bucket ="{}device-nodes/dt={}/hr={}/".format(s3_bucket, dt, hr)
    device_nodes_df = spark.read.parquet(s3_device_nodes_bucket)

    s3_device_edges_bucket = "{}device-edges/dt={}/hr={}/".format(s3_bucket, dt, hr)
    device_edges_df = spark.read.parquet(s3_device_edges_bucket)

    cols = ['source',
            'target',
            'sourcePort',
            'targetPort',
            'sourceIp',
            'targetIp',
            # 'sourceName',
            # 'targetName',
            # 'sourceVendor',
            # 'targetVendor',
            # 'sourceVersion',
            # 'targetVersion',
            'createdAt',
            'relType',
            'id',
            'from',
            'to',
            # 'DFTemporalDedupeTransformer_max_temporal_interval',
            'lastSeenAt',
            'expiredAt',
            'lastModifiedAt']
    device_edges_df.select(cols).show()

    device_edges_df_1 = device_edges_df.select(F.col("source").alias("source1"),
                                               F.col("sourcePort").alias("sourcePort1"),
                                               F.col("target").alias("target1"),
                                               F.col("targetPort").alias("targetPort1"))

    device_edges_df_join = device_edges_df.join(device_edges_df_1,
                                             [device_edges_df.target==device_edges_df_1.source1,
                                              device_edges_df.targetPort==device_edges_df_1.sourcePort1
                                              ],
                                             how="inner")
    device_bidirectional_df = device_edges_df_join.filter("source == target1 and sourcePort == targetPort1")

    device_bidirectional_df.count()

    #print uniq-edges
    device_edges_df.select("source", "target").groupBy("source", "target").count().count()

    device_bidirectional_df.select("orgId", "siteId").groupBy("orgId", "siteId").count().count()

    # device_bidirectional_df.select("orgId", "siteId", "source", "sourcePort",  "target", "targetPort")

    macs = list(device_bidirectional_df.select("source", "target").toPandas()['source'])
    macs1 = list(device_bidirectional_df.select("source", "target").toPandas()['target'])
    bidirection_macs = set(macs + macs1)

    device_nodes_df_macs = device_nodes_df.filter(device_nodes_df.mac.isin(bidirection_macs))
    device_nodes_df_macs.select("model", "statDeviceType", "deviceType",  "vendor")\
        .groupBy("model", "statDeviceType", "deviceType",  "vendor")\
        .count()\
        .orderBy(F.col("model").desc())\
        .show(50)

    # device_edges_df_0.filter(F.col("source") == F.col("target1")).count()

def test_circle_device_graph_per_site(dt="2021-02-26", hr="03"):
    import pyspark.sql.functions as F
    dt="2021-03-04"; hr="11"
    s3_device_nodes_bucket = "s3://mist-aggregated-stats-production/" \
                             "aggregated-stats/graph/snapshots/" \
                             "device-nodes/dt={}/hr={}/".format(dt, hr)
    device_nodes_df = spark.read.parquet(s3_device_nodes_bucket)
    device_nodes_df = (device_nodes_df \
                      .withColumn("message",F.array(F.col("id")))
                      )

    s3_device_edges_bucket = "s3://mist-aggregated-stats-production/" \
                             "aggregated-stats/graph/snapshots/" \
                             "device-edges/dt={}/hr={}/".format(dt, hr)
    device_edges_df = spark.read.parquet(s3_device_edges_bucket) \
        .selectExpr("from as src", "to as dst", "*")

    # sites = device_nodes_df.select("siteId").agg(F.collect_set("siteId").alias("sites")).collect()
    sites = list(device_nodes_df.select("siteId").agg(F.collect_set("siteId").alias("sites"))
                 .select("sites").toPandas()['sites'][0])

    sites = ["7b6d794d-e0e2-4607-a88d-3c5294f67686"]
    site = sites[0]
    cycles_set = set()
    for site in sites[:100]:
        device_nodes_df_site = device_nodes_df.filter(F.col('siteId')==site)
        if device_nodes_df_site.count()>2:
            device_edges_df_site = device_edges_df.filter(F.col('siteId')==site)

            g_site = GraphFrame(device_nodes_df_site, device_edges_df_site)
            cycles, cycle_statistics = find_cycles(g_site, 10)
            if cycles.count() > 0 and cycles.filter(F.size("cycle")>2).count()> 0:
                print("cycle-site ", site)
                # cycles.show(truncate=False)
                cycles.filter(F.size("cycle") > 2).show(truncate=False)
                cycle_statistics
                cycles_set.add(site)
    cycles_set

    result = g_site.stronglyConnectedComponents(maxIter=10)
    result.select("id", "component").orderBy("component").show()

    cols = ['source',
            'target',
            'sourcePort',
            'targetPort',
            'sourceIp',
            'targetIp',
            # 'sourceName',
            # 'targetName',
            # 'sourceVendor',
            # 'targetVendor',
            # 'sourceVersion',
            # 'targetVersion',
            'createdAt',
            'relType',
            'id',
            'from',
            'to',
            # 'DFTemporalDedupeTransformer_max_temporal_interval',
            'lastSeenAt',
            'expiredAt',
            'lastModifiedAt']
    device_edges_df_site.select(cols).show()

def check_loop_site():
    import pyspark.sql.functions as F
    dt="2021-03-04"; hr="11"
    s3_device_nodes_bucket = "s3://mist-aggregated-stats-production/" \
                             "aggregated-stats/graph/snapshots/" \
                             "device-nodes/dt={}/hr={}/".format(dt, hr)
    device_nodes_df = spark.read.parquet(s3_device_nodes_bucket)

    s3_device_edges_bucket = "s3://mist-aggregated-stats-production/" \
                             "aggregated-stats/graph/snapshots/" \
                             "device-edges/dt={}/hr={}/".format(dt, hr)
    device_edges_df = spark.read.parquet(s3_device_edges_bucket)

    device_nodes_df = (device_nodes_df \
                       .withColumn("message",F.array(F.col("id")))
                       )

    device_edges_df = device_edges_df \
        .selectExpr("from as src", "to as dst", "*")

    site = '1ef1d02d-c049-4f13-8d44-71b3654f75fd'
    site = '6d6aa55f-3e36-4f57-8924-e80c7e135529'  # A->B->C-A
    device_nodes_df_site = device_nodes_df.filter(F.col('siteId')==site)
    device_edges_df_site = device_edges_df.filter(F.col('siteId')==site)


    # device_edges_df_pd = device_edges_df_site.toPandas()
    # g = nx.from_pandas_edgelist(device_edges_df_pd,  'source', 'target')


    device_nodes_df_site.select("siteId", 'name', "mac", "model", "name", "deviceType", "vendor",
                                "createdAt", "expiredAt", "lastSeenAt", "lastModifiedAt")\
        .orderBy(F.col("model").desc(), F.col("lastModifiedAt")).show(truncate=False)

    cols = ['sourceName',           'targetName',
            'source',            'target',
            'sourcePort',            'targetPort',
            'sourceIp',            'targetIp',
            'createdAt',            'relType',
            # 'id',
            # 'from',            'to',
            'lastSeenAt',
            'expiredAt',
            'lastModifiedAt']

    device_edges_df_site.select(cols).orderBy("source", "target").show()

    g_site = GraphFrame(device_nodes_df_site, device_edges_df_site)
    # cycles, cycle_statistics = find_cycles(g_site, 10)
    # if cycles.count()>0:
    #     print("cycle-site ", site)
    #     cycles.show(truncate=False)
    #     cycle_statistics
    #     cycles_set.add(site)
    # cycles_set

    result = g_site.stronglyConnectedComponents(maxIter=10)
    result.select("id", "mac", "component").orderBy("component").show()

    components= list(result.select("id", "mac", "name",  "component")\
                     .orderBy("component").groupBy("component").count().filter("count>1").select("component").toPandas()['component'])

    problematic_macs = list( result.filter(result.component.isin(components)).select("mac").toPandas()['mac'])

    # problematic_macs = list( result.filter(F.col("component")==42949672960).select("mac").toPandas()['mac'])
    device_edges_df_site_probematic = device_edges_df_site\
        .filter(device_edges_df.source.isin(problematic_macs)&device_edges_df.target.isin(problematic_macs))

    # device_edges_df_site_probematic.show()
    device_edges_df_site_probematic.select(cols).show()


def test_uplink():
    date = '{2021-03-1[12]}'
    env  = "production"
    cloud= 's3'
    s3_path =cloud+'://mist-aggregated-stats-'+env +'/aggregated-stats/switch_uplink_model/dt='+date+'/*/last_3_hour/*.parquet'
    df = spark.read.parquet(s3_path)

    site = '6d6aa55f-3e36-4f57-8924-e80c7e135529'  # A->B->C-A
    problematic_macs = ['b033a61c3180', '64c3d6a65400']
    site = '76d6f4b2-ce21-4b80-bfef-b2cc06d8b4ca'  # AE0
    problematic_macs = ['f07cc7cafb93', '4c16fc50d700']
    df_site = df.filter(F.col("site_id")==site)
    df_selected = df_site.filter(df_site.switch_id.isin(problematic_macs))
    cols = ["switch_id", "name",  "device_type", "timestamp", "uplink_prob_score",
            "uplink_value_threshold", "uplink_score_rank", "uplink_value",
            "bridge", "remotecap","bridge_port_count"]
    df_selected.select(cols).orderBy(F.col("uplink_value").desc(), F.col("timestamp").asc()).show()

    AE0_1 = ['xe-0/2/0', 'xe-1/2/0']  # f07cc7cafb93
    AE0_2 = ['xe-0/0/10', 'xe-1/0/10']  # 4c16fc50d700
    df_selected.filter(df_selected.name.isin(AE0_1)).select(cols).orderBy(F.col("uplink_value").desc(), F.col("timestamp").asc()).show()
    df_selected.filter(df_selected.name.isin(AE0_2)).select(cols).orderBy(F.col("uplink_value").desc(), F.col("timestamp").asc()).show()


def test_device_graph():
    g = graph_prep("2021-02-26", "03")

    sites = list(device_nodes_df.select("siteId").agg(F.collect_set("siteId").alias("sites")).select("sites").toPandas()['sites'][0])

    cycles= g.pregel \
        .setMaxIter(5) \
        .withVertexColumn("is_cycle", F.lit(""), F.lit("logic to be added")) \
        .withVertexColumn("sequence", F.lit(""), Pregel.msg()) \
        .sendMsgToDst(Pregel.src("id")) \
        .aggMsgs(F.collect_list(Pregel.msg())) \
        .run()

    cycles.count()  # 401014

    cycles.filter(F.col("sequence").isNotNull()).select("orgId", "siteId")\
        .groupBy("orgId", "siteId").count()\
        .orderBy(F.col("count").desc())\
        .show(truncate=False)

    # site  13537/23720 =
    cycles.filter(F.col("sequence").isNotNull()).select("orgId", "siteId")\
        .groupBy("orgId", "siteId").count().count()

    cycles.show()
    cycles_sites =cycles.select("siteId", F.col("sequence").isNotNull().alias("circle"))\
        .groupBy("siteId", "circle").count()
    cycles_sites.show()

    cycles_g = cycles.select(F.col("sequence").isNotNull().alias("circle")).groupBy("circle").count()
    has_circle = cycles_g.cycles_g.filter(F.col("circle")==True).count() > 0
    if has_circle:
        cycles_g.show()

    for site in sites:
        device_nodes_df_site = device_nodes_df.filter(F.col('siteId')==site)
        if device_nodes_df.count()>2:
            device_edges_df_site = device_edges_df.filter(F.col('siteId')==site)

            g = GraphFrame(device_nodes_df_site, device_edges_df_site)

        # g.vertices.count()
        # g.edges.count()
        #
        # g.vertices.show()
        # g.edges.show()

        cycles = g.pregel \
            .setMaxIter(5) \
            .withVertexColumn("is_cycle", F.lit(""), F.lit("logic to be added")) \
            .withVertexColumn("sequence", F.lit(""), Pregel.msg()) \
            .sendMsgToDst(Pregel.src("id")) \
            .aggMsgs(F.collect_list(Pregel.msg())) \
            .run()

        cycles_g = cycles.select(F.col("sequence").isNotNull().alias("circle")).groupBy("circle").count()
        has_circle = cycles_g.cycles_g.filter(F.col("circle")==True).count() > 0
        if has_circle:
            print("site=", site)
            cycles_g.show()

        # cycles.show()
        # cycles.select(F.col("sequence").isNotNull().alias("circle")).groupBy("circle").count().show()


def test_site():
    gs_bucket = 'gs://mist-secorapp-production/dimension/site/'
    df = spark.read.parquet(gs_bucket)


def test_graph_tesst():
    s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/graph_test/"
    s3_connectivity_bucket = s3_bucket + "ap-stats-analytics/connectivity/dt=2021-03-15/hr=00/"
    connectivity_df = spark.read.parquet(s3_connectivity_bucket)
    connectivity_df.printSchema()

    s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/graph_test/"
    s3_connectivity_enriched_bucket = s3_bucket + "oc-stats-analytics/connectivity_enriched/dt=2021-03-15/hr=02/"
    connectivity_enriched_df = spark.read.parquet(s3_connectivity_enriched_bucket)
    connectivity_enriched_df.printSchema()
    connectivity_enriched_df.select("relationship").groupBy("relationship").count().show()

    s3_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/graph/"
    s3_connectivity_enriched_bucket = s3_bucket + "oc-stats-analytics/connectivity_enriched/dt=2021-03-16/hr=21/"
    connectivity_enriched_df = spark.read.parquet(s3_connectivity_enriched_bucket)
    connectivity_enriched_df.printSchema()
    connectivity_enriched_df.select("relationship").groupBy("relationship").count().show()
    connectivity_enriched_df.select("switch_id",  "relationship").groupBy("switch_id")\
        .agg(F.max("relationship").alias("relationship_max")).groupBy("relationship_max").count().show()

# def test_from_to():
#     dt="2021-02-26"; hr="03"
#     s3_device_edges_bucket = "s3://mist-aggregated-stats-production/" \
#                              "aggregated-stats/graph/snapshots/" \
#                              "device-edges/dt={}/hr={}/".format(dt, hr)
#     device_edges_df = spark.read.parquet(s3_device_edges_bucket)
#     device_edges_df.filter(F.col("from")==F.col("to")).count()


if __name__ == "main":
    dt="2021-02-26"; hr="03"
    test_circle_device_graph_per_site(dt, hr)
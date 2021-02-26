
#
# ./dev-scripts/okta_spark jupyter-notebook --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11
#
# spark_ipython  --name wenfeng --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11
#
def test_device_graph():
    from graphframes import GraphFrame
    from graphframes.lib import *
    import pyspark.sql.functions as F

    dt = "dt=2021-02-25"
    hr = "hr=03"
    s3_device_nodes_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/graph/snapshots/" \
                             "device-nodes/{}/{}/".format(dt, hr)
    device_nodes_df = spark.read.parquet(s3_device_nodes_bucket)

    s3_device_edges_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/graph/snapshots/" \
                             "device-edges/{}/{}/".format(dt, hr)
    device_edges_df = spark.read.parquet(s3_device_edges_bucket)\
        .selectExpr("from as src", "to as dst", "*")

    g = GraphFrame(device_nodes_df, device_edges_df)

    sites = device_nodes_df.select("siteId").agg(F.collect_set("siteId").alias("sites")).collect()

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



#
# ./dev-scripts/okta_spark jupyter-notebook --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11
#
def test_device_graph():
    dt = "dt=2021-02-25"
    hr = "hr=03"
    s3_device_nodes_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/graph/snapshots/" \
                             "device-nodes/{}/{}/".format(dt, hr)
    device_nodes_df = spark.read.parquet(s3_device_nodes_bucket)

    s3_device_edges_bucket = "s3://mist-aggregated-stats-production/aggregated-stats/graph/snapshots/" \
                             "device-edges/{}/{}/".format(dt, hr)
    device_edges_df = spark.read.parquet(s3_device_edges_bucket)

    device_edges_df= device_edges_df.selectExpr("from as src", "to as dst", "*")
    #
    from graphframes import *
    g = GraphFrame(device_nodes_df, device_edges_df)

    g.vertices.show()
    g.edges.show()



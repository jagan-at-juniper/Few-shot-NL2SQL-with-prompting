
def test_loop():
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql import SQLContext
    import pyspark.sql.functions as F

    from graphframes import GraphFrame
    from graphframes.lib import *

    SimpleCycle=[
        ("1","2"),
        ("2","3"),
        ("3","4"),
        ("4","5"),
        ("5","2"),
        ("5","6")
    ]

    edges = sqlContext.createDataFrame(SimpleCycle,["src","dst"]) \
        .withColumn("self_loop",F.when(F.col("src")== F.col("dst"),True).otherwise(False))
    edges.show()


    vertices=edges.select("src").union(edges.select("dst"))\
        .distinct()\
        .distinct()\
        .withColumnRenamed('src', 'id')
    #vertices = spark.createDataFrame([[1], [2], [3], [4],[5],[6],[7],[8],[9]], ["id"])

    #vertices.sort("id").show()
    graph = GraphFrame(vertices, edges)

    # cycles= graph.pregel \
    #     .setMaxIter(5) \
    #     # .withVertexColumn("is_cycle", F.lit(""), F.lit("logic to be added")) \
    #     .withVertexColumn("sequence", F.lit(""), Pregel.msg()) \
    #     .sendMsgToDst(Pregel.src("id")) \
    #     .aggMsgs(F.collect_list(Pregel.msg())) \
    #     .run()

    cycles = graph.pregel \
        .setMaxIter(5) \
        .withVertexColumn("is_cycle", F.lit(""), F.lit("logic to be added")) \
        .withVertexColumn("sequence", F.lit(""), Pregel.msg()) \
        .sendMsgToDst(Pregel.src("id")) \
        .aggMsgs(F.collect_list(Pregel.msg())) \
        .run()


    cycles.show()
    cycles_g = cycles.select(F.col("sequence").isNotNull().alias("circle")).groupBy("circle").count()
    has_circle = cycles_g.cycles_g.filter(F.col("circle")==True).count() > 0
    if has_circle:
        cycles_g.show()



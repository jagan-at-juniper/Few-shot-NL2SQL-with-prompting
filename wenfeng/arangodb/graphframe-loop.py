# graphframes modules

import pyspark.sql.functions as F
from graphframes import GraphFrame
from graphframes.lib import *
from graphframes.lib import AggregateMessages as AM
# AM=AggregateMessages

from pyspark.sql.types import *

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
        newVertices.sort("id").show(truncate=False)

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
    vertices= (vertices\
               .withColumn("message",F.array(F.col("id")))
                    )
    graph = GraphFrame(vertices, edges)

    # cycles= graph.pregel \
    #     .setMaxIter(5) \
    #     # .withVertexColumn("is_cycle", F.lit(""), F.lit("logic to be added")) \
    #     .withVertexColumn("sequence", F.lit(""), Pregel.msg()) \
    #     .sendMsgToDst(Pregel.src("id")) \
    #     .aggMsgs(F.collect_list(Pregel.msg())) \
    #     .run()

    # cycles = graph.pregel \
    #     .setMaxIter(5) \
    #     .withVertexColumn("is_cycle", F.lit(""), F.lit("logic to be added")) \
    #     .withVertexColumn("sequence", F.lit(""), Pregel.msg()) \
    #     .sendMsgToDst(Pregel.src("id")) \
    #     .aggMsgs(F.collect_list(Pregel.msg())) \
    #     .run()
    #
    # cycles.show()
    # cycles_g = cycles.select(F.col("sequence").isNotNull().alias("circle")).groupBy("circle").count()
    # has_circle = cycles_g.cycles_g.filter(F.col("circle")==True).count() > 0
    # if has_circle:
    #     cycles_g.show()



    SimpleCycle2=[
        ("1","2"),
        ("1","3"),
        ("2","4"),
        ("3","5"),
        ("3","6"),
        ("6","7")
    ]

    edges2 = sqlContext.createDataFrame(SimpleCycle2,["src","dst"]) \
        .withColumn("self_loop", F.when(F.col("src")== F.col("dst"),True).otherwise(False))
    vertices2 = edges2.select("src").union(edges2.select("dst")) \
        .distinct() \
        .distinct() \
        .withColumnRenamed('src', 'id') \
        .withColumn("message",F.array(F.col("id")))

    graph2 = GraphFrame(vertices2, edges2)

    # cycles2 = graph2.pregel \
    #     .setMaxIter(5) \
    #     .withVertexColumn("is_cycle", F.lit(""), F.lit("logic to be added")) \
    #     .withVertexColumn("sequence", F.lit(""), Pregel.msg()) \
    #     .sendMsgToDst(Pregel.src("id")) \
    #     .aggMsgs(F.collect_list(Pregel.msg())) \
    #     .run()
    #
    # cycles2.show()

    result = graph2.connectedComponents()
    result.select("id", "component").orderBy("component").show()


    cycles = sqlContext.createDataFrame(sc.emptyRDD(),
                                    StructType([StructField("cycle",ArrayType(StringType()),True)]))

    cycles, cycle_statistics = find_cycles(graph, 10)
    cycles.show()
    cycle_statistics



    cycles_2, cycle_statistics_2 = find_cycles(graph2, 10)

    cycles_2.show()
    cycle_statistics
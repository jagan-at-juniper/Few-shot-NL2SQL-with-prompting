import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType, ArrayType, StringType
import networkx as nx


def load_data(date, hour):
    ENV = 'production'
    component = 'device'
    prefix_edges = f"s3://mist-aggregated-stats-{ENV}/aggregated-stats/graph/snapshots/{component}-edges/"
    prefix_nodes = f"s3://mist-aggregated-stats-{ENV}/aggregated-stats/graph/snapshots/{component}-nodes/"
    df_edges = readParquet(prefix_edges, dates=date, hour=hour)
    df_nodes = readParquet(prefix_nodes, dates=date, hour=hour)
    return df_edges, df_nodes


def process_data(df_edges, df_nodes):
    """
    * Filter out deviceType=='ap'
    * create src, dst columns with mapping
        src-->(source,sourcePort)
        dst-->(target,targetPort)
    """
    df_edges = df_edges.withColumn('isExpired', F.when(F.isnull(F.col('expiredAt')), 'False').otherwise('True'))
    df_active_edges = df_edges.filter(F.col('isExpired') == False)
    df_active_edges = df_active_edges.filter(F.col('relType') == 'uplink')

    df_nodes = df_nodes.withColumn('isExpired', F.when(F.isnull(F.col('expiredAt')), 'False').otherwise('True'))
    df_active_nodes = df_nodes.filter(F.col('isExpired') == False)
    df_active_nodes = df_active_nodes.withColumnRenamed("siteId", "siteId_copy")

    df_join = df_active_edges.join(df_active_nodes, [df_active_edges.source == df_active_nodes.mac,
                                                     df_active_edges.siteId == df_active_nodes.siteId_copy]) \
        .drop('siteId_copy')

    df_active_nodes = df_active_nodes.withColumn('mac_target', df_active_nodes.mac) \
        .withColumn('deviceType_target', df_active_nodes.deviceType)

    df_join = df_join.join(df_active_nodes.select('mac_target', 'deviceType_target', 'siteId_copy'),
                           [df_join.target == df_active_nodes.mac_target,
                            df_join.siteId == df_active_nodes.siteId_copy])

    df_active_edges_filtered = df_join.filter((F.col('deviceType') != 'ap') & (F.col('deviceType_target') != 'ap')) \
        .select('siteId', 'source', 'target', 'sourcePort', 'targetPort', 'sourceVendor', 'targetVendor', 'deviceType',
                'deviceType_target', 'relType')

    df_active_edges_filtered = df_active_edges_filtered.withColumn('src', F.concat_ws("__", F.col('source'),
                                                                                      F.col('sourcePort'))) \
        .withColumn('dst', F.concat_ws("__", F.col('target'), F.col('targetPort')))

    return df_active_edges_filtered


def get_sites_with_cycles(data_frame):
    df_site_graph = get_site_graph(data_frame)
    df_site_graph_cycle = df_site_graph.withColumn('cycle_detected', udf_isCycle(F.col('graph')))
    return df_site_graph_cycle.filter(F.col('cycle_detected') == True)

    # TODO: add logger statement
    # cycle_detect = df_site_graph_cycle_true.count()
    # print(f'Sites with cycles in {component} collection = {cycle_detect}')


def get_simple_cycles(date, hour):
    df_active_edges, df_active_nodes = load_data(date=date, hour=hour)
    df_filtered = process_data(df_active_edges, df_active_nodes)
    df_graph = get_sites_with_cycles(df_filtered)
    df_all_cycles = df_graph.withColumn('cycles', udf_findCycles(F.col('graph')))
    # TODO: persist df_all_cycles to cloud storage + send stats to SFX


"""
Cycle detection functions
"""


def get_site_graph(data_frame):
    df_site_adj_list = data_frame.select('siteId', 'src', 'dst') \
        .groupby('siteId', 'src') \
        .agg(F.collect_set(F.col('dst')).alias('dst'))
    df_site_adj_list = df_site_adj_list.groupby("siteId") \
        .agg(F.map_from_arrays(F.collect_list("src"), F.collect_list("dst")).alias("graph"))

    return df_site_adj_list


def _isCycle(graph, v, done, stack):
    done[v] = True
    stack[v] = True

    for neighbour in graph[v]:
        if neighbour in graph:
            if not done[neighbour]:
                if _isCycle(graph, neighbour, done, stack):
                    print(f'Part of cycle: {neighbour}')
                    return True
            elif stack[neighbour]:
                print(f'Last neighbour found on stack: {neighbour}')
                return True

    stack[v] = False
    return False


def isCycle(graph):
    src_vertices = graph.keys()
    done = {k: False for k in src_vertices}
    stack = {k: False for k in src_vertices}

    for node in src_vertices:
        if not done[node]:
            if _isCycle(graph, node, done, stack):
                return True
    return False


udf_isCycle = F.udf(isCycle, BooleanType())


class Cycles:
    def __init__(self, graph):
        self._graph = graph
        self.nodes = self.getNodes()
        self.nodeMap = {node: i for i, node in enumerate(self.nodes)}
        self.visited = ['NOT_VISITED' for _ in range(len(self.nodes))]  # Initialize all nodes unvisited
        self.stack = []  # Stack to keep track of visited nodes
        self.cycles = []

    def getNodes(self):
        _nodes = set([])
        for k, v in self._graph.items():
            items = [k] + v
            for item in items:
                _nodes.add(item)
        return list(_nodes)

    def printCycles(self):
        return self.cycles

    def addCycle(self, v):
        cycle = []
        cycle.append(self.nodes[self.stack[-1]])
        i = 1
        while cycle[-1] != v and i < len(self.stack):
            i += 1
            cycle.append(self.nodes[self.stack[-i]])

        self.cycles.append(cycle)

    def dfs(self):
        curr = self.stack[-1]
        if self.nodes[curr] in self._graph:
            for neighbour in self._graph[self.nodes[curr]]:
                to = self.nodeMap[neighbour]
                if self.visited[to] == 'ON_STACK':
                    self.addCycle(to)
                elif self.visited[to] == 'NOT_VISITED':
                    self.stack.append(to)
                    self.visited[to] = 'ON_STACK'
                    self.dfs()

        self.visited[curr] = 'DONE'
        self.stack.pop()

    def findCycles(self):
        for i, node in enumerate(self.nodes):
            if self.visited[i] == 'NOT_VISITED':
                self.stack = []
                self.stack.append(i)
                self.visited[i] = 'ON_STACK'
                self.dfs()

        return self.printCycles()


# def find_cycles(graph):
#     cycles = Cycles(graph)
#     return cycles.findCycles()

def find_cycles(graph):
    g = nx.DiGraph(graph)
    return list(nx.simple_cycles(g))


udf_findCycles = F.udf(find_cycles, ArrayType(ArrayType(StringType())))

"""
Utility functions
"""


def readParquet(prefix, dates="{*}", hour="{*}", fields=None):
    path = prefix + "/dt=" + dates + "/hr=" + hour + "/"
    return spark.read.parquet(path)

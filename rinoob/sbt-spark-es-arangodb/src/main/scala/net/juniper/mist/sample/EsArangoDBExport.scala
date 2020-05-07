package net.juniper.mist.sample

import com.arangodb.spark.{ArangoSpark, WriteOptions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit}

object EsArangoDBExport {
  // TODO read from config
  lazy val ArangoHost: String = "ec2-18-212-187-115.compute-1.amazonaws.com:8529"
  lazy val ArangoUser: String = "arango"
  lazy val ArangoPassword: String = "Echo123"
  lazy val ArangoDB: String = "arango"
  lazy val ArangoCollectionDoc: String = "collection_doc"
  lazy val ArangoCollectionEdge: String = "collection_edge"
  lazy val ESHost: String = "es7-access-000-staging:9200"
  lazy val ESIndex: String = "switch_topology_202004"
  implicit lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session")
      .config("arangodb.hosts", ArangoHost)
      .config("arangodb.user", ArangoUser)
      .config("arangodb.password", ArangoPassword)
      .config("spark.broadcast.compress", "true")
      .config("es.nodes", ESHost)
      .getOrCreate()
  }

  def readEsData(implicit spark: SparkSession): DataFrame = {
    spark.read.format("org.elasticsearch.spark.sql")
      .option("pushdown", "true")
      .load(ESIndex)
      .toDF()
  }

  def saveToArango(data: DataFrame, collection: String) {
    ArangoSpark.saveDF(data, collection, WriteOptions(ArangoDB))
  }

  def getNodesFromEdgelist(data: DataFrame): DataFrame = {
    val df_left =  data.select(col("source").as("_key"),
      col("source_device_type").as("device_type"),
      col("source_interface").as("interface"),
      col("source_uplink_score").as("uplink_score")
    )
    val df_right =  data.select(col("target").as("_key"),
      col("target_device_type").as("device_type"),
      col("target_interface").as("interface"),
      col("target_uplink_score").as("uplink_score")
    )
    df_left.unionByName(df_right)
  }

  def getEdgesFromEgeList(data: DataFrame): DataFrame = {
    data.select(concat(lit(ArangoCollectionDoc + "/"), col("source")).as("_from"),
      concat(lit(ArangoCollectionDoc + "/"), col("target")).as("_to"),
      lit("downlink").as("type"))

  }

  def main(args: Array[String]): Unit = {
    val df: DataFrame = readEsData
    val nodes: DataFrame = getNodesFromEdgelist(df)
    saveToArango(nodes, ArangoCollectionDoc)
    val edges: DataFrame = getEdgesFromEgeList(df)
    saveToArango(edges, ArangoCollectionEdge)
  }
}
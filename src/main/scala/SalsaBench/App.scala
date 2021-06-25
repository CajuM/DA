package SalsaBench


import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.matching.Regex

import java.net.URL

import com.martinkl.warc.mapreduce.WARCInputFormat
import com.martinkl.warc.WARCWritable

import org.jsoup.Jsoup

import org.apache.hadoop.io.LongWritable

import org.apache.spark.rdd._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.storage.StorageLevel

import org.apache.spark.graphx._


object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.classesToRegister", "org.apache.hadoop.io.LongWritable")
      .appName("SalsaBench")
      .getOrCreate()

    args(0) match {
      case "subgraph" => new SalsaBench(spark).subgraph(args(1))
      case "salsa" => new SalsaBench(spark).salsa(args(1), args(2), args(3).toInt)
    }
  }
}

class SalsaBench(spark: SparkSession) {
  import spark.implicits._

  val sc = spark.sparkContext

  def subgraph(sample: String) {
    val data = getDF(sample).cache
    val graph = getGraph(data).cache

    val out = largestConnectedComponent(graph).cache

    out.vertices.coalesce(1).toDF.write.format("json").save("vertices")
    out.edges.coalesce(1).toDF.write.format("json").save("edges")
  }

  def salsa(verticesPath: String, edgesPath: String, numIter: Int) {
    val vertices: RDD[(VertexId, (String, String))] = spark.read.json(verticesPath)
      .rdd
      .map { case row => {
        val id = row.getAs[VertexId]("_1")
        val attr = row.getAs[Row]("_2")

        val url = attr.getAs[String]("_1")
        val ntype = attr.getAs[String]("_2")

        (id, (url, ntype))
      }}

    val edges: RDD[Edge[Null]] = spark.read.json(edgesPath)
      .rdd
      .map { case row => Edge[Null](
        row.getAs[VertexId]("srcId"),
        row.getAs[VertexId]("dstId"),
        null
      )}

    val graph: Graph[(String, String), Null] = Graph(vertices, edges)

    val numVertices = graph.numVertices
    val numAuthorities = graph
      .vertices
      .filter { case (_, (_, ntype)) => ntype == "authority" }
      .count
    val numHubs = numVertices - numAuthorities

    println(f"Hubs: ${numHubs}; Authorities: ${numAuthorities};")

    var rankGraph: Graph[Double, (Double, Double)] = graph
      .mapVertices { case (id, (url, ntype)) => (ntype, 0) }
      .outerJoinVertices(graph.inDegrees) { case (id, (ntype, _), in) => {
        val indeg = in match {
          case Some(deg) => deg
          case None => 0
        }

        (ntype, indeg)
      }}
      .outerJoinVertices(graph.outDegrees) { case (id, (ntype, indeg), out) => {
        val outdeg = out match {
          case Some(deg) => deg
          case None => 0
        }

        val deg = ntype match {
          case "authority" => indeg
          case "hub" => outdeg
        }

        (ntype, 1.0 / deg)
      }}
      .mapTriplets(edge => (edge.srcAttr._2, edge.dstAttr._2), new TripletFields(true, true, false))
      .mapVertices { case (id, (ntype, _)) => { ntype match {
        case "authority" => 1.0 / numAuthorities
        case "hub" => 1.0 / numHubs
      }}}
      .cache

    rankGraph.vertices.foreachPartition(x => {})
    rankGraph.edges.foreachPartition(x => {})

    def salsaSendMsg(edge: EdgeTriplet[Double, (Double, Double)]): Iterator[(VertexId, Double)] = {
      val srank = edge.srcAttr
      val drank = edge.dstAttr

      val (sideg, dideg) = edge.attr

      val srcMsg = drank * dideg
      val dstMsg = srank * sideg

      Iterator((edge.srcId, srcMsg), (edge.dstId, dstMsg))
    }

    def salsaMergeMsg(a: Double, b: Double): Double = a + b

    def salsaVertexProgram(id: VertexId, attr: Double, msg: Double): Double = if (msg == Double.NegativeInfinity) attr else msg

    val startTime = System.nanoTime()

    rankGraph = Pregel(rankGraph, Double.NegativeInfinity, numIter * 2)(salsaVertexProgram, salsaSendMsg, salsaMergeMsg)

    rankGraph.vertices.foreachPartition(x => {})

    val endTime = System.nanoTime()
    val totalTime = (endTime -  startTime).toDouble / 10e9

    println(f"Time: ${totalTime}")

    val rv = graph
      .mapVertices { case (id, (url, ntype)) => (url, ntype, 0.0) }
      .outerJoinVertices(rankGraph.vertices) { case (id, (url, ntype, _), rank) => (url, ntype, rank.getOrElse(0.0)) }
      .vertices
      .cache

    println("Authorities:")
    rv
      .filter { case (_, (_, ntype, _)) => ntype == "authority" }
      .takeOrdered(10)(Ordering[Double].reverse on { case ( _, (_, _, rank)) => rank })
      .foreach { case (_, (url, _, rank)) => println(f"${url} ${rank}") }

    println("Hubs:")
    rv
      .filter { case (_, (_, ntype, _)) => ntype == "hub" }
      .takeOrdered(10)(Ordering[Double].reverse on { case ( _, (_, _, rank)) => rank })
      .foreach { case (_, (url, _, rank)) => println(f"${url} ${rank}") }
  }

  def largestConnectedComponent(graph: Graph[(String, String), Null]): Graph[(String, String), Null] = {
    val ccs = graph
      .connectedComponents
      .cache

    val largestCcId = ccs
      .vertices
      .map { case (id, ccId) => (ccId, 1L) }
      .reduceByKey(_+_)
      .reduce {
        case ((ccId1, count1), (ccId2, count2)) =>
          if (count1 > count2) (ccId1, count1) else (ccId2, count2)
      }
      ._1

    return ccs
      .outerJoinVertices(graph.vertices) {
        case (id, ccId, attrOpt) => if (ccId == largestCcId) attrOpt else None
      }
      .subgraph(vpred = (id, attrOpt) => attrOpt.isDefined )
      .mapVertices { (id, attrOpt) => attrOpt match {
        case Some(attr) => attr
        case None => throw new RuntimeException()
      }}
  }

  def getDF(path: String): Dataset[Row] = {
    sc.newAPIHadoopFile(
      path,
      classOf[WARCInputFormat],
      classOf[LongWritable],
      classOf[WARCWritable],
    )
    .map { case (k, v) => (k, v.getRecord()) }
    .filter { case (k, v) => {
      val hdr = v.getHeader()

      hdr.getRecordType() == "response"
    }}
    .flatMap { case (k, v) => {
      Try {
        val url = new URL(v.getHeader().getTargetURI())
        val content = new String(v.getContent())

        val links = Jsoup
          .parse(content)
          .select("a[href]")
          .asScala
          .flatMap { case e => { Try { new URL(url, e.attr("href")) }.toOption }}
          .filter { case url2 => Seq("http", "https").contains(url2.getProtocol()) }
          .map { case url2 => url2.toString }
          .distinct

        (url.toString, links)
      }
      .toOption
    }}
    .toDF("url", "links")
    .repartition($"url")
  }

  def getGraph(data: Dataset[Row]): Graph[(String, String), Null] = {
    val hubs = data
      .where(size($"links") > 0)
      .select($"url")
      .distinct
      .withColumn("type", lit("hub"))
      .cache

    val authorities = data
      .select(explode($"links").as("url"))
      .distinct
      .join(data.select($"url".as("url1")), ($"url" === $"url1"))
      .select($"url")
      .distinct
      .withColumn("type", lit("authority"))
      .cache

    val vertexDF = hubs
      .union(authorities)
      .rdd
      .zipWithIndex
      .map { case row => (row._1.getAs[String]("url"), row._1.getAs[String]("type"), row._2) }
      .toDF("url", "type", "id")
      .cache

    val edgeDF = data
      .select($"url".as("url1"), explode($"links").as("url2"))
      .distinct
      .join(vertexDF, ($"url" === $"url1") && ($"type" === "hub"))
      .drop("url", "url1", "type")
      .withColumnRenamed("id", "id1")
      .join(vertexDF, ($"url" === $"url2") && ($"type" === "authority"))
      .drop("url", "url2", "type")
      .withColumnRenamed("id", "id2")

    val vertexRDD = vertexDF.rdd
      .map { case row => (
        row.getAs[Long]("id"),
        (
          row.getAs[String]("url"),
          row.getAs[String]("type")
        )
      )}
      .repartition(sc.defaultParallelism)

    val edgeRDD = edgeDF.rdd
      .map { case row  => Edge[Null](
        row.getAs[Long]("id1"),
        row.getAs[Long]("id2"),
        null
      )}
      .repartition(sc.defaultParallelism)

    Graph(vertexRDD, edgeRDD)
  }
}

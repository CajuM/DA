package PageRankBench


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
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.graphx.lib.PageRank


object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.classesToRegister", "org.apache.hadoop.io.LongWritable")
      .appName("PageRankBench")
      .getOrCreate()

    args(0) match {
      case "subgraph" => new PageRankBench(spark).subgraph(args(1))
      case "pagerank" => new PageRankBench(spark).pagerank(args(1), args(2), args(3).toInt)
    }
  }
}

class PageRankBench(spark: SparkSession) {
  import spark.implicits._

  val sc = spark.sparkContext

  def subgraph(sample: String) {
    val data = getDF(sample).cache
    val graph = getGraph(data).cache

    val out = largestConnectedComponent(graph).cache

    out.vertices.coalesce(1).toDF.write.format("json").save("vertices")
    out.edges.coalesce(1).toDF.write.format("json").save("edges")
  }

  def pagerank(verticesPath: String, edgesPath: String, numIter: Int) {
    val vertices: RDD[(VertexId, String)] = spark.read.json(verticesPath)
      .rdd
      .map { case row => {
        val id = row.getAs[VertexId]("_1")
        val url = row.getAs[String]("_2")

        (id, url)
      }}

    val edges: RDD[Edge[Null]] = spark.read.json(edgesPath)
      .rdd
      .map { case row => Edge[Null](
        row.getAs[VertexId]("srcId"),
        row.getAs[VertexId]("dstId"),
        null
      )}

    val graph: Graph[String, Null] = Graph(vertices, edges)

    val numVertices = graph.numVertices

    println(f"Vertices: ${numVertices};")

    graph.vertices.foreachPartition(x => {})
    graph.edges.foreachPartition(x => {})

    val graph2 = graph.mapVertices { case (_, _) => null }

    graph2.vertices.foreachPartition(x => {})
    graph2.edges.foreachPartition(x => {})

    graph2.partitionBy(PartitionStrategy.RandomVertexCut)

    val startTime = System.nanoTime()

    val rankGraph = PageRank.run(graph2, numIter)

    val rv = graph
      .mapVertices { case (id, url) => (url, 0.0) }
      .outerJoinVertices(rankGraph.vertices) { case (id, (url,  _), rank) => (url, rank.getOrElse(0.0)) }
      .vertices

    println("Ranks:")
    rv
      .takeOrdered(10)(Ordering[Double].reverse on { case ( _, (_, rank)) => rank })
      .foreach { case (_, (url, rank)) => println(f"${url} ${rank}") }

    val endTime = System.nanoTime()
    val totalTime = (endTime -  startTime).toDouble / 10e9

    println(f"Time: ${totalTime}")
  }

  def largestConnectedComponent(graph: Graph[String, Null]): Graph[String, Null] = {
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
    .repartition(sc.defaultParallelism)
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
  }

  def getGraph(data: Dataset[Row]): Graph[String, Null] = {
    val vertices = data
      .select(explode($"links").as("url"))
      .distinct
      .union(data.select(data("url")).where(size(data("links")) =!= 0))
      .distinct
      .cache

    val vertexDF = vertices
      .rdd
      .zipWithIndex
      .map { case row => (row._1.getAs[String]("url"), row._2) }
      .toDF("url", "id")
      .cache

    val edgeDF = data
      .select($"url".as("url1"), explode($"links").as("url2"))
      .distinct
      .join(vertexDF, ($"url" === $"url1"))
      .drop("url", "url1")
      .withColumnRenamed("id", "id1")
      .join(vertexDF, ($"url" === $"url2"))
      .drop("url", "url2")
      .withColumnRenamed("id", "id2")

    val vertexRDD = vertexDF.rdd
      .map { case row => (
        row.getAs[Long]("id"),
        row.getAs[String]("url")
      )}

    val edgeRDD = edgeDF.rdd
      .map { case row  => Edge[Null](
        row.getAs[Long]("id1"),
        row.getAs[Long]("id2"),
        null
      )}

    Graph(vertexRDD, edgeRDD)
  }
}

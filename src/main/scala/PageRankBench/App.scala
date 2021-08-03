package PageRankBench


import org.apache.spark.sql.SparkSession

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.util.GraphGenerators


object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("PageRankBench")
      .getOrCreate()

    new PageRankBench(spark).pagerank(args(0).toInt, args(1).toInt, args(2).toInt)
  }
}

class PageRankBench(spark: SparkSession) {
  import spark.implicits._

  val sc = spark.sparkContext

  def pagerank(numVertices: Int, numEdges: Int, numIter: Int) {
    val graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, numVertices, numEdges)

    val realVertices = graph.numVertices
    val realEdges = graph.numEdges

    println(f"Vertices: ${realVertices};")
    println(f"Edges: ${realEdges};")

    graph.partitionBy(PartitionStrategy.RandomVertexCut)

    graph.vertices.foreachPartition(x => {})
    graph.edges.foreachPartition(x => {})

    val startTime = System.nanoTime()

    val rankGraph = PageRank.run(graph.cache, numIter)

    println("Ranks:")
    rankGraph
      .vertices
      .takeOrdered(10)(Ordering[Double].reverse on { case (_, rank) => rank })
      .foreach { case (vid, rank) => println(f"${vid} ${rank}") }

    val endTime = System.nanoTime()
    val totalTime = (endTime -  startTime).toDouble / 10e9

    println(f"Time: ${totalTime}")
  }
}

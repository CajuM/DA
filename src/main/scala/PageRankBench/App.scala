package PageRankBench


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import org.apache.spark.graphx.{Graph, GraphLoader, PartitionStrategy, TripletFields}
import org.apache.spark.graphx.lib.PageRank


object App {
  def main(args: Array[String]): Unit = {
    val logTag = "[PageRankBench]"

    val conf = new SparkConf()
      .setAppName("PageRankBench")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val pgr = args(0) match {
      case "myPgR" => myPageRank(_, _, _)
      case "sparkPgR" => PageRank.run[Int, Int](_, _, _)
    }

    val edgesFile = args(1)
    val numIter = args(2).toInt
    val partMul = args(3).toInt

    println(s"${logTag} function=${args(0)}; dataset=${args(1)}; iterations=${args(2)}; partitionsx=${args(3)};")

    val unPartitionedGraph: Graph[Int, Int] = GraphLoader
      .edgeListFile(sc, edgesFile)

    val graph = Graph(
      unPartitionedGraph.vertices.repartition(sc.defaultParallelism * partMul),
      unPartitionedGraph.edges.repartition(sc.defaultParallelism * partMul)
    )
      .partitionBy(PartitionStrategy.RandomVertexCut)

    graph.vertices.foreachPartition(x => {})
    graph.edges.foreachPartition(x => {})

    val realVertices = graph.numVertices
    val realEdges = graph.numEdges

    println(s"${logTag} Vertices: ${realVertices};")
    println(s"${logTag} Edges: ${realEdges};")

    val startTime = System.nanoTime()

    val rankGraph = pgr(graph, numIter, 0.15)

    println(s"${logTag} Ranks:")
    rankGraph
      .vertices
      .takeOrdered(10)(Ordering[Double].reverse on { case (_, rank) => rank })
      .foreach { case (vid, rank) => println(s"${logTag} ${vid} ${rank}") }

    val endTime = System.nanoTime()
    val totalTime = (endTime -  startTime).toDouble / 10e9

    println(s"${logTag} Time: ${totalTime}")
    sc.stop()
  }

  def myPageRank(graph: Graph[Int, Int], numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] = {
    var rankGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (_, _, deg) => deg.getOrElse(0) }
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      .mapVertices { (_, _) => 1.0 }

    var rankVertices = rankGraph.vertices.cache
    val rankEdges = rankGraph.edges.cache

    rankVertices.foreachPartition(x => {})
    rankEdges.foreachPartition(x => {})

    for { _ <- 1 to numIter } {
      val prevRankVertices = rankVertices

      val tmpGraph = Graph(rankVertices, rankEdges)

      rankVertices = tmpGraph
        .aggregateMessages[Double](
          ctx => {
            val rank = resetProb + (1.0 - resetProb) * ctx.srcAttr * ctx.attr
            ctx.sendToDst(rank)
          },
          _ + _,
          TripletFields.Src
        )
        .cache

      rankVertices.foreachPartition(x => {})

      prevRankVertices.unpersist()
      tmpGraph.vertices.unpersist()
    }

    Graph(rankVertices, rankEdges)
  }
}

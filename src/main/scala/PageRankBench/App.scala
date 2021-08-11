package PageRankBench


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.TripletFields
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.impl.GraphImpl


object App {
  val logTag = "[PageRankBench]"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("PageRankBench")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val alg = args(0) match {
      case "mySALSA1" => mySALSA1(_, _)
      case "mySALSA2" => mySALSA2(_, _)
      case "mySALSA3" => mySALSA3(_, _)
    }

    val edgesFile = args(1)
    val numIter = args(2).toInt
    val partMul = args(3).toInt

    println(s"${logTag} Function: ${args(0)}; Dataset: ${args(1)}; Iterations: ${args(2)}; Partitionsx: ${args(3)};")

    val unPartitionedGraph: Graph[Int, Int] = GraphLoader
      .edgeListFile(sc, edgesFile)

    val graph = Graph(
      unPartitionedGraph.vertices.repartition(sc.defaultParallelism * partMul),
      unPartitionedGraph.edges.repartition(sc.defaultParallelism * partMul)
    )
      .partitionBy(PartitionStrategy.RandomVertexCut)
      .cache

    graph.vertices.foreachPartition(x => {})
    graph.edges.foreachPartition(x => {})

    val realVertices = graph.numVertices
    val realEdges = graph.numEdges

    println(s"${logTag} Vertices: ${realVertices};")
    println(s"${logTag} Edges: ${realEdges};")

    val startTime = System.nanoTime()

    val rankGraph = alg(graph, numIter)

    println(s"${logTag} Authority ranks:")
    rankGraph
      .vertices
      .takeOrdered(10)(Ordering[Double].reverse on { case (_, (arank: Double, _)) => arank })
      .foreach { case (vid, (arank, _)) => println(s"${logTag}   ${vid} ${arank}") }

    println(s"${logTag} Hub ranks:")
    rankGraph
      .vertices
      .takeOrdered(10)(Ordering[Double].reverse on { case (_, (_, hrank: Double)) => hrank })
      .foreach { case (vid, (_, hrank)) => println(s"${logTag}   ${vid} ${hrank}") }

    val endTime = System.nanoTime()
    val totalTime = (endTime -  startTime).toDouble / 1e9

    println(s"${logTag} Time: ${totalTime}")
    sc.stop()
  }

  def mySALSA1(graph: Graph[Int, Int], numIter: Int): Graph[(Double, Double), (Double, Double)] = {
    val numAuthorities = graph
      .outerJoinVertices(graph.inDegrees) { (_, _, deg) => deg match {
        case Some(_) => 1
        case None => 0
      } }
      .vertices
      .reduce { case ((_, cnt1), (_, cnt2)) => (0L, cnt1 + cnt2) }
      ._2

    val numHubs = graph
      .outerJoinVertices(graph.outDegrees) { (_, _, deg) => deg match {
        case Some(_) => 1
        case None => 0
      } }
      .vertices
      .reduce { case ((_, cnt1), (_, cnt2)) => (0L, cnt1 + cnt2) }
      ._2

    println(s"${logTag} Authorities: ${numAuthorities}; Hubs: ${numHubs};")

    var rankGraph: Graph[(Double, Double), (Double, Double)] = graph
      .outerJoinVertices(graph.outDegrees) { (_, _, outdeg) => outdeg.getOrElse(0) }
      .outerJoinVertices(graph.inDegrees) { (_, outdeg, indeg) => (indeg.getOrElse(0), outdeg) }
      .mapTriplets(e => (1.0 / e.dstAttr._1, 1.0 / e.srcAttr._2), TripletFields.All)
      .mapVertices { (_, _) => (1.0 / numAuthorities, 1.0 / numHubs) }
      .cache

    for { _ <- 1 to numIter } {
      val prevRankGraph = rankGraph

      val arankVertices1 = rankGraph
        .aggregateMessages[(Double, Double)](
          ctx => {
            val (darank, _) = ctx.dstAttr
            val (indeg, _) = ctx.attr

            ctx.sendToSrc((darank * indeg, 0.0))
          },
          { case ((arank1, _), (arank2, _)) => (arank1 + arank2, 0.0) },
          TripletFields.Dst
        )

      val arankVertices = rankGraph
        .outerJoinVertices(arankVertices1) { (_, _, rank) => rank.getOrElse((0.0, 0.0)) }
        .aggregateMessages[(Double, Double)](
          ctx => {
            val (sparank, _) = ctx.srcAttr
            val (_, outdeg) = ctx.attr

            ctx.sendToDst((sparank * outdeg, 0.0))
          },
          { case ((arank1, _), (arank2, _)) => (arank1 + arank2, 0.0) },
          TripletFields.Src
        )

      val hrankVertices1 = rankGraph
        .aggregateMessages[(Double, Double)](
          ctx => {
            val (_, dhrank) = ctx.srcAttr
            val (_, outdeg) = ctx.attr

            ctx.sendToDst((0.0, dhrank * outdeg))
          },
          { case ((_, hrank1), (_, hrank2)) => (0.0, hrank1 + hrank2) },
          TripletFields.Src
        )

      val hrankVertices = rankGraph
        .outerJoinVertices(hrankVertices1) { (_, _, rank) => rank.getOrElse((0.0, 0.0)) }
        .aggregateMessages[(Double, Double)](
          ctx => {
            val (_, sphrank) = ctx.dstAttr
            val (indeg, _) = ctx.attr

            ctx.sendToSrc((0.0, sphrank * indeg))
          },
          { case ((_, hrank1), (_, hrank2)) => (0.0, hrank1 + hrank2) },
          TripletFields.Dst
        )

      rankGraph = rankGraph
        .outerJoinVertices(arankVertices) { case (_, _, arank) => arank.getOrElse((0.0, 0.0)) }
        .outerJoinVertices(hrankVertices) { case (_, arank, hrank) => (arank._1, hrank.getOrElse(0.0, 0.0)._2) }

      rankGraph.cache
      rankGraph.edges.foreachPartition(x => {})

      prevRankGraph.vertices.unpersist()
    }

    rankGraph
  }

  def mySALSA2(graph: Graph[Int, Int], numIter: Int): Graph[(Double, Double), (Double, Double)] = {
    val numAuthorities = graph
      .outerJoinVertices(graph.inDegrees) { (_, _, deg) => deg match {
        case Some(_) => 1
        case None => 0
      } }
      .vertices
      .reduce { case ((_, cnt1), (_, cnt2)) => (0L, cnt1 + cnt2) }
      ._2

    val numHubs = graph
      .outerJoinVertices(graph.outDegrees) { (_, _, deg) => deg match {
        case Some(_) => 1
        case None => 0
      } }
      .vertices
      .reduce { case ((_, cnt1), (_, cnt2)) => (0L, cnt1 + cnt2) }
      ._2

    println(s"${logTag} Authorities: ${numAuthorities}; Hubs: ${numHubs};")

    var rankGraph: Graph[(Double, Double), (Double, Double)] = graph
      .outerJoinVertices(graph.outDegrees) { (_, _, outdeg) => outdeg.getOrElse(0) }
      .outerJoinVertices(graph.inDegrees) { (_, outdeg, indeg) => (indeg.getOrElse(0), outdeg) }
      .mapTriplets(e => (1.0 / e.dstAttr._1, 1.0 / e.srcAttr._2), TripletFields.All)
      .mapVertices { (_, _) => (1.0 / numAuthorities, 1.0 / numHubs) }
      .cache

    for { _ <- 1 to numIter } {
      val prevRankGraph = rankGraph

      val rankVertices1 = rankGraph
        .aggregateMessages[(Double, Double)](
          ctx => {
            val (_, shrank) = ctx.srcAttr
            val (darank, _) = ctx.dstAttr
            val (indeg, outdeg) = ctx.attr

            ctx.sendToSrc((darank * indeg, 0.0))
            ctx.sendToDst((0.0, shrank * outdeg))
          },
          { case ((arank1, hrank1), (arank2, hrank2)) => (arank1 + arank2, hrank1 + hrank2) },
          TripletFields.All
        )

      val rankVertices = rankGraph
        .outerJoinVertices(rankVertices1) { (_, _, rank) => rank.getOrElse((0.0, 0.0)) }
        .aggregateMessages[(Double, Double)](
          ctx => {
            val (sparank, _) = ctx.srcAttr
            val (_, sphrank) = ctx.dstAttr
            val (indeg, outdeg) = ctx.attr

            ctx.sendToDst((sparank * outdeg, 0.0))
            ctx.sendToSrc((0.0, sphrank * indeg))
          },
          { case ((arank1, hrank1), (arank2, hrank2)) => (arank1 + arank2, hrank1 + hrank2) },
          TripletFields.All
        )

      rankGraph = rankGraph
        .outerJoinVertices(rankVertices) { case (_, _, rank) => rank.getOrElse(0.0, 0.0) }

      rankGraph.cache
      rankGraph.edges.foreachPartition(x => {})

      prevRankGraph.vertices.unpersist()
    }

    rankGraph
  }

  def mySALSA3(graph: Graph[Int, Int], numIter: Int): Graph[(Double, Double), (String, Double)] = {
    val inDegrees = graph.inDegrees.cache
    val outDegrees = graph.outDegrees.cache

    val dedges = graph
      .outerJoinVertices(outDegrees) { (_, _, outdeg) => outdeg.getOrElse(0) }
      .outerJoinVertices(inDegrees) { (_, outdeg, indeg) => (indeg.getOrElse(0), outdeg) }
      .mapTriplets(e => (e.dstAttr._1, e.srcAttr._2), TripletFields.All)
      .edges
      .cache

    val rddEdges = dedges
      .map { case Edge(srcId, dstId, attr) => (srcId, (dstId, attr)) }
      .cache

    val rrddEdges = dedges
      .map { case Edge(srcId, dstId, attr) => (dstId, (srcId, attr)) }
      .cache

    val aedges = rddEdges
      .join(rddEdges)
      .map {
        case (_, (v1, v2)) => {
          val (srcId, _) = v1
          val (dstId, (indeg, outdeg)) = v2

          Edge(srcId, dstId, ("authority", 1.0 / indeg / outdeg))
        }
      }

    val hedges = rrddEdges
      .join(rrddEdges)
      .map {
        case (_, (v1, v2)) => {
          val (srcId, _) = v1
          val (dstId, (indeg, outdeg)) = v2

          Edge(srcId, dstId, ("hub", 1.0 / outdeg / indeg))
        }
      }

    val numAuthorities = inDegrees.count()
    val numHubs = outDegrees.count()

    println(s"${logTag} Authorities: ${numAuthorities}; Hubs: ${numHubs};")

    val rankEdges = aedges.union(hedges)

    var rankGraph: Graph[(Double, Double), (String, Double)] = Graph
      .fromEdges(rankEdges, (1.0 / numAuthorities, 1.0 / numHubs))
      .cache

    inDegrees.unpersist()
    outDegrees.unpersist()

    rddEdges.unpersist()
    rrddEdges.unpersist()

    dedges.unpersist()

    for { _ <- 1 to numIter } {
      val prevRankGraph = rankGraph

      val rankVertices = rankGraph
        .aggregateMessages[(Double, Double)](
          ctx => {
            val (darank, dhrank) = ctx.dstAttr
            val (etype, p) = ctx.attr

            etype match {
              case "authority" => ctx.sendToSrc((p * darank, 0.0))
              case "hub" => ctx.sendToSrc((0.0, p * dhrank))
            }
          },
          { case ((arank1, hrank1), (arank2, hrank2)) => (arank1 + arank2, hrank1 + hrank2) },
          TripletFields.Dst
        )

      rankGraph = rankGraph
        .outerJoinVertices(rankVertices) { case (_, _, rank) => rank.getOrElse(0.0, 0.0) }

      rankGraph.cache
      rankGraph.edges.foreachPartition(x => {})

      prevRankGraph.vertices.unpersist()
    }

    rankGraph
  }
}

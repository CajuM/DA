package PiBench


import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.SparkSession


object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("PiBench")
      .getOrCreate()

    new PiBench(spark).run(args(0).toLong)
  }
}

class PiBench(spark: SparkSession) {
  import spark.implicits._

  val sc = spark.sparkContext

  def run(population: Long) = {
    val numPartitions = (population.toDouble / Int.MaxValue).ceil.toInt * sc.defaultParallelism
    val space = RandomRDDs.uniformVectorRDD(sc, population, 2, numPartitions)

    val startTime = System.nanoTime()

    val pi = space
      .map((v: Vector) => {
        val x = v(0)
        val y = v(1)

        if ((x * x + y * y) > 1.0) 0 else 1
      })
      .sum
      .toDouble / population * 4.0

    val endTime = System.nanoTime()
    val runTime = (endTime - startTime) / 1e9

    println(f"[PiBench] Pi is aproximately: ${pi}")
    println(f"[PiBench] Calculated in: ${runTime}")
  }
}

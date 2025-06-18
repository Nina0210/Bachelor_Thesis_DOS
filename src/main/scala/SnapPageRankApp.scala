import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.PrintWriter


object SnapPageRankApp {

  def main(args: Array[String]): Unit = {
    println("Starting SNAP PageRank App")

    val applicationSTartTime = System.nanoTime()

    val edgeListFilePath: String = if (args.length > 0) args(0) else {
      println("WARN: No edge list file path provided as a command-line argument.")
      println("WARN: Using default path.")
      "Graphs/wiki-Vote.txt"
    }

    val tolerance = 0.001
    val resetProb = 0.15
    val topNUsersToShow = 20

    println("----- Configurations -----")
    println(s"Tolerance: $tolerance")
    println(s"Reset probability: $resetProb")

    val spark = SparkSession.builder
      .appName("SnapPageRankApp")
      //.master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    println(s"Loading graph form edge list : $edgeListFilePath")

    val edgesRDD: RDD[Edge[Int]] = sc.textFile(edgeListFilePath)
      .filter(line => !line.trim.startsWith("#") && line.trim.nonEmpty)
      .map { line =>
        val parts = line.split("\\s+")
        if (parts.length >= 2) {
          try {
            val srcId = parts(0).toLong
            val dstId = parts(1).toLong
            Edge(srcId, dstId, 1)
          } catch {
            case e: NumberFormatException =>
              println(s"WARN: Could not parse line: $line. Error: §{e.getMessage}")
              null
          }
        } else {
          println(s"WARN: Line with insufficient parts: $line")
          null
        }
      }.filter(_ != null)

    // create the graph
    val graph = Graph.fromEdges(edgesRDD, defaultValue = 1)
    println(s"\nGraph constructed with ${graph.numVertices} vertices and ${graph.numEdges} edges.")
    if (graph.numVertices == 0 || graph.numEdges == 0) {
      println("ERROR: Graph is empty. Check file path and format.")
      spark.stop()
      return
    }

    // run PageRank
    println(s"Running PageRank (Tolerance: $tolerance, Reset Probability: $resetProb)...")
    val pageRankGraph = graph.pageRank(tol = tolerance, resetProb = resetProb)

    // get top N PageRank Scores
    println(s"\nTop $topNUsersToShow PageRank Scores:")
    val topRanks = pageRankGraph.vertices
      .sortBy(_._2, ascending = false)
      .take(topNUsersToShow)


    topRanks.foreach { case (vertexId, rank) =>
    println(f"  Vertex ID: $vertexId%-10s PageRank: $rank%.6f")
      //writer.println(s"$vertexId,$rank")
    }

    //writer.close()

    val applicationEndTime = System.nanoTime()
    val totalRunTimeNanos = applicationEndTime - applicationSTartTime
    val totalRuntimeSeconds = totalRunTimeNanos / 1e9
    println(f"Total Application Runtime: $totalRuntimeSeconds seconds")

    // stop spark session
    println("\nStopping Spark Session.")
    spark.stop()
  }

}

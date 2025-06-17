import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.util.Random
import scala.concurrent.duration._

object MonteCarloPageRankApp {

  def main(args: Array[String]): Unit = {
    println("Starting Monte Carlo PageRank Application")

    val applicationSTartTime = System.nanoTime()

    // Configuration
    val edgeListFilePath: String = if (args.length > 0) args(0) else {
      println("WARN: No edge list file path provided. Using default path.")
      // default path
      "/tmp/wiki-Talk.txt"
    }

    val numWalkersPerNodeFactor = 10
    val numSteps = 30
    val resetProb = 0.15
    val topNToDisplay = 20

    println("----- Configurations -----")
    println(s"Number of walker per node: ${numWalkersPerNodeFactor}r")
    println(s"Number of steps: $numSteps")
    println(s"Reset probability: $resetProb")



    val spark = SparkSession.builder
      .appName("MonteCarloPageRankApp")
      // running locally
      //.master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    println(s"Loading graph from edge list: $edgeListFilePath")

    // Load Edges and Create Graph Adjacency List
    val edgesRDD: RDD[(Long, Long)] = sc.textFile(edgeListFilePath)
      .filter(line => !line.trim.startsWith("#") && line.trim.nonEmpty)
      .flatMap { line =>
        val parts = line.split("\\s+")
        if (parts.length >= 2) {
          try {
            Some((parts(0).toLong, parts(1).toLong))
          } catch {
            case _: NumberFormatException =>
              println(s"WARN: Could not parse line: $line")
              None
          }
        } else {
          println(s"WARN: Line with insufficient parts: $line")
          None
        }
      }
    // Graph Structure
    val adjList: RDD[(Long, Iterable[Long])] = edgesRDD.groupByKey().cache()
    val allNodesRDD: RDD[Long] = sc.union(edgesRDD.map(_._1), edgesRDD.map(_._2)).distinct()
    val allNodesArray: Array[Long] = allNodesRDD.collect().sorted

    if (allNodesArray.isEmpty) {
      println("ERROR: No nodes found in the graph. Check file path and format.")
      spark.stop()
      return
    }
    val numGraphNodes = allNodesArray.length
    println(s"Graph has $numGraphNodes unique nodes identified.")
    val numEdges = edgesRDD.count()
    println(s"Number of edges: $numEdges")


    // Initialize random Walkers
    val totalWalkers = numGraphNodes * numWalkersPerNodeFactor
    val desiredPartitions = sc.defaultParallelism * 8
    val broadcastNodes = sc.broadcast(allNodesArray)
    var walkersRDD: RDD[Long] = sc.parallelize(
      0 until totalWalkers, desiredPartitions).mapPartitions { iter =>
      val localNodes = broadcastNodes.value
      val localNumGraphNodes = localNodes.length
      val random = new Random(System.nanoTime + Thread.currentThread.getId)
      iter.map(_ => localNodes(random.nextInt(localNumGraphNodes)))
    }
      .cache()

    println(s"Initialized $totalWalkers random walkers across ${walkersRDD.getNumPartitions} partitions.")

    // Simulate Random Walks
    for (step <- 1 to numSteps) {
      val startTimeStep = System.nanoTime()
      println(s"Simulating step $step of $numSteps...")

      val nextWalkersRDD = walkersRDD
        .map(walkerNodeId => (walkerNodeId, 1))
        .leftOuterJoin(adjList)
        .flatMap { case (currentNodeId, (_, optionalNeighbors)) =>
          val localAllNodes = broadcastNodes.value
          val localNumGraphNodes = localAllNodes.length
          val randomForStep = new Random(System.nanoTime + Thread.currentThread().getId + step + currentNodeId.toInt)
          val randValue = randomForStep.nextDouble()
          if (randValue < resetProb) {
            Seq(localAllNodes(randomForStep.nextInt(localNumGraphNodes)))
          } else {
            optionalNeighbors match {
              case Some(neighbors) if neighbors.nonEmpty =>
                val neighborsArray = neighbors.toArray
                Seq(neighborsArray(randomForStep.nextInt(neighborsArray.length)))
              case _ => // Dangling node or node not in adjList's keys
                Seq(localAllNodes(randomForStep.nextInt(localNumGraphNodes)))
            }
          }
        }.repartition(walkersRDD.getNumPartitions)
        .cache()

      // Trigger an action to materialize nextWalkersRDD and allow unpersisting the old one
      nextWalkersRDD.count()

      walkersRDD.unpersist(blocking = false)
      walkersRDD = nextWalkersRDD
      val endTimeStep = System.nanoTime()
      println(s"Step $step completed in ${(endTimeStep - startTimeStep) / 1e9d} seconds.")
    }

    // Calculate PageRank Approximations
    val pageRankEstimates: RDD[(Long, Double)] = walkersRDD
      .map(nodeId => (nodeId, 1.0))
      .reduceByKey(_ + _)
      .mapValues(count => count / totalWalkers.toDouble)

    println(s"\nTop $topNToDisplay Monte Carlo PageRank Estimates (after $numSteps steps):")
    val topRanks = pageRankEstimates
      .sortBy(_._2, ascending = false)
      .take(topNToDisplay)

    topRanks.foreach { case (vertexId, rank) =>
      println(f"  Vertex ID: $vertexId%-10s Approx. PageRank: $rank%.6f")
    }

    val applicationEndTime = System.nanoTime()
    val totalRuntimeNanos = applicationEndTime - applicationSTartTime
    val totalRuntimeSeconds = totalRuntimeNanos/1e9d
    println(f"Total Application Runtime: $totalRuntimeSeconds seconds")

    // Stop SparkSession
    println("\nStopping Spark Session.")
    if(walkersRDD != null) walkersRDD.unpersist(blocking = true)
    if (adjList != null) adjList.unpersist(blocking = true)
    spark.stop()
  }
}
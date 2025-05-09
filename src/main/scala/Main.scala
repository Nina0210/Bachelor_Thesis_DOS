import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    println("Starting Simple PageRank Application")

    val spark = SparkSession.builder
      .appName("SimplePageRankApp")
      // For running locally in IntelliJ without a separate cluster:
       .master("local[*]")
      // For submitting to your standalone cluster, remove/comment .master()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN") // Reduce verbose Spark logging

    // 1. Define Vertices: (VertexID, VertexAttribute - e.g., a name)
    //    Let's use simple names for our pages/nodes.
    val vertices: RDD[(VertexId, String)] = sc.parallelize(Seq(
      (1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E")
    ))

    // 2. Define Edges: Edge(SourceVertexID, DestinationVertexID, EdgeAttribute - can be anything, even 1)
    //    These represent links between pages.
    val edges: RDD[Edge[Int]] = sc.parallelize(Seq(
      Edge(1L, 2L, 1), Edge(1L, 3L, 1), // A links to B and C
      Edge(2L, 3L, 1),                 // B links to C
      Edge(3L, 1L, 1),                 // C links to A (creating a cycle)
      Edge(3L, 4L, 1),                 // C links to D
      Edge(4L, 5L, 1),                 // D links to E
      Edge(5L, 4L, 1)                  // E links to D (another cycle)
    ))

    // 3. Create the Graph
    //    If a vertex is referenced in edges but not in vertices, a default attribute will be used.
    //    Let's define a default attribute for such cases.
    val defaultVertexAttr = "Unknown Page"
    val graph = Graph(vertices, edges, defaultVertexAttr)

    println(s"\nOriginal Graph has ${graph.numVertices} vertices and ${graph.numEdges} edges.")
    println("Vertices:")
    graph.vertices.collect().sortBy(_._1).foreach(println)
    println("Edges:")
    graph.edges.collect().foreach(println)

    // 4. Run PageRank
    //    .pageRank(tolerance, resetProbability)
    //    tolerance: The PageRank algorithm iterates until scores converge.
    //               This is the tolerance for convergence (e.g., 0.001).
    //               A smaller value means more iterations but higher accuracy.
    //    resetProbability: The probability of a random jump (damping factor), typically 0.15.
    //                      This means 1 - resetProbability (0.85) is the probability of following a link.
    val tolerance = 0.001
    val resetProb = 0.15
    val pageRankGraph: Graph[Double, Double] = graph.pageRank(tol = tolerance, resetProb = resetProb)

    // The pageRankGraph now has vertex attributes as their PageRank scores
    // and edge attributes as normalized weights (contribution of src to dst PageRank).

    println(s"\nPageRank Scores (Tolerance: $tolerance, Reset Probability: $resetProb):")

    // Join PageRank scores with original vertex names for better readability
    val ranksWithNames = graph.vertices.join(pageRankGraph.vertices)
      .map { case (id, (name, rank)) => (name, rank) }
      .sortBy(_._2, ascending = false) // Sort by rank descending

    ranksWithNames.collect().foreach { case (name, rank) =>
      println(f"  Page '$name%s' has PageRank: $rank%.4f") // Format rank to 4 decimal places
    }

    // You can also inspect the edge attributes if interested
    // println("\nPageRank Edge Weights:")
    // pageRankGraph.edges.collect().foreach(edge =>
    //   println(s"  Edge from ${edge.srcId} to ${edge.dstId} has weight: ${edge.attr}%.4f")
    // )

    // 5. Stop SparkSession
    println("\nStopping Spark Session.")
    spark.stop()
  }
}
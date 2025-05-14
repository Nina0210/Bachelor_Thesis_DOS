import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.io.Source



object SimpleGraphXApp {
  def main(args: Array[String]): Unit = {
    println("Hello world 5!")

    println("Starting Simple GraphX App")

    // 1. Create a SparkSession (the entry point for Spark applications)
    // .master("local[*]") runs Spark locally using all available cores
    val spark = SparkSession.builder
      .appName("SimpleGraphXApp")
      .master("local[*]") // Run locally
      // Optional: Add config options if needed
      // .config("spark.driver.memory", "2g")
      .getOrCreate()

    // Get the SparkContext from the SparkSession
    val sc = spark.sparkContext
    sc.setLogLevel("WARN") // Reduce verbose Spark logging

    // 2. Create example data: Vertices (ID, Attribute) and Edges (SrcID, DstID, Attribute)
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Seq(
        (3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))
      ))

    val relationships: RDD[Edge[String]] =
      sc.parallelize(Seq(
        Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")
      ))

    // Define a default user in case relationships reference missing users
    val defaultUser = ("John Doe", "Missing")

    // 3. Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    println(s"\nGraph has ${graph.numVertices} vertices and ${graph.numEdges} edges.")

    // 4. Perform some basic GraphX operations
    println("\nVertices where attribute is 'prof':")
    graph.vertices.filter { case (id, (name, pos)) => pos == "prof" }.collect().foreach {
      case (id, (name, pos)) => println(s"  $name (ID: $id) is a $pos")
    }

    println("\nEdges where the relationship is 'advisor':")
    graph.edges.filter(_.attr == "advisor").collect().foreach { edge =>
      println(s"  Edge from ${edge.srcId} to ${edge.dstId} is type ${edge.attr}")
    }

    println("\nTriplets (Source Vertex -> Edge -> Destination Vertex):")
    graph.triplets.collect().foreach { triplet =>
      println(s"  ${triplet.srcAttr._1} (${triplet.srcAttr._2}) --[${triplet.attr}]--> ${triplet.dstAttr._1} (${triplet.dstAttr._2})")
    }

    // Example: Count degrees
    println("\nVertex Degrees (in, out, total):")
    graph.degrees.join(graph.inDegrees).join(graph.outDegrees)
      .join(graph.vertices) // Join with vertices to get names
      .map { case (id, (((totalDeg, inDeg), outDeg), (name, pos))) =>
        s"  $name (ID: $id): In=$inDeg, Out=$outDeg, Total=$totalDeg"
      }
      .collect().foreach(println)


    // 5. Stop the SparkSession
    println("\nStopping Spark Session.")
    spark.stop()
  }
}
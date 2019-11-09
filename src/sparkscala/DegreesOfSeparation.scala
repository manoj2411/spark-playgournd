import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import scala.util.control.Breaks._

// # 9
object DegreesOfSeparation extends App  {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "degreesOfSeparation")
  /*
      * row -> (id, (connectionIds, distance = 9999, Colour = White))
      * startId -> Gray, distance = 0
      * ALGO: Map - Reduce
        - Map
          * look for gray node
          * expand it: create new gray nodes for each connection with empty connections
          * mark it Black (processed)
          * update distance as we go through
        - Reduce
          * Combine all node for same heroId
          * select most dark color
          * merge all connections
          * select least distance

      * accumulator - allows many executors to increment shared variable
        var hitCounter: LongAccumulator("hitCounter")
  * */
  val startId = 5306 // SipderMan
  val targetId = 14 // ADAM
  var hitCounter: LongAccumulator = sc.longAccumulator("hitCounter")
  val black = "Black"
  val gray = "Gray"
  val white = "White"

  type BFSData = (IndexedSeq[Int], Int, String)
  type BFSNode = (Int, BFSData)

  val lines = sc.textFile("./data/Marvel-graph.txt")
  var rddBFSNodes : RDD[BFSNode] = lines.map(generateBFSNode) // starting nodes with 1 gray node

  breakable {
    for (iteration <- 1 to 10) {

      println("Running BFS Iteration# " + iteration)
      val mapped: RDD[BFSNode] = rddBFSNodes.flatMap(bfsMap) // return expanded nodes for current gray nodes
      println("Processing " + mapped.count() + " values.")
      // check if hitCounter is non-zeros, means we found our destination node
      if (hitCounter.value > 0) {
        println(s"Hit the target character! From ${hitCounter.value} different direction(s).")
        break
      }
      rddBFSNodes = mapped.reduceByKey(bfsReduce)
    }
  }


  /*    Methods    */

  /*
     Reduce
     * Combine all node for same heroId
     * select most dark color
     * merge all connections
     * select least distance

  */
  def bfsReduce(node1: BFSData, node2: BFSData): BFSData = {
    var (collection, distance, color) = node1
    val (collection2, distance2, color2) = node2

    if (collection2.nonEmpty) collection ++= collection2
    if (distance > distance2) distance = distance2

    if (color2 == black || (color2 == gray && color == white))
      color = color2

    (collection, distance, color)
  }

  /*
     Map
     * look for gray node
     * expand it: create new gray nodes for each connection with empty connections
     * mark it Black (processed)
     * update distance as we go through
  */
  def bfsMap(node: BFSNode): IndexedSeq[BFSNode] = {
    val (superHeroId, nodeData) = node
    val (connections, distance, _) = nodeData
    var (_,_,color) = nodeData
    var result: IndexedSeq[BFSNode] = IndexedSeq()

    if (color == gray) {
      result = connections.map { connectionId =>
        if (connectionId == targetId)
          hitCounter.add(1)
        (connectionId, (IndexedSeq(), distance + 1, gray))
      }
      color = black
    }
    result :+ (superHeroId,(connections, distance, color))
  }

  def generateBFSNode(line: String): BFSNode = {
    val fields = line.split("\\s+")
    val id = fields(0).trim.toInt
    val connections: IndexedSeq[Int] = for (i <- 1 until fields.length) yield fields(i).trim.toInt

    var distance = 9999
    var colour = white
    if (id == startId) {
      distance = 0
      colour = gray
    }
    (id, (connections, distance, colour))
  }
}



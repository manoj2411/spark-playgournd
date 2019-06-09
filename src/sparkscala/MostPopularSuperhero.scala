package sparkscala

import java.nio.charset.CodingErrorAction

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.{Codec, Source}

object MostPopularSuperhero extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def getHeroNames = {
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val input = Source.fromFile("./data/Marvel-names.txt").getLines
    var names = Map[String, String]()
    input.foreach { line =>
      // TODO: correct parsing, do it on bases of \"
      val fields = line.split(" ")
      names += (fields(0) -> fields(1))
    }
    names
  }

  val sc = new SparkContext("local[*]", "MostPopularSuperhero")

  val heroNames = sc.broadcast(getHeroNames)

  val input = sc.textFile("./data/Marvel-graph.txt")

  // Map(heroId -> noOfFriends)
  val superherosMap = input.map { line =>
    // TODO: correct parsing, do it on bases of \s+
    val row = line.split(" ")
    (row(0), row.length - 1)
  }

  val superherosFriends = superherosMap.reduceByKey(_ + _)

  val sortedResult = superherosFriends.sortBy(_._2).collect

  sortedResult.foreach { x => println(s"${heroNames.value(x._1)} -> ${x._2}") }

}

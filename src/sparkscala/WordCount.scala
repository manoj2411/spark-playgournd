package sparkscala

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.log4j._

object WordCount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "WordCount")
  val lines = sc.textFile("./data/book.txt")
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.countByValue()

  wordCounts.foreach(println)

}

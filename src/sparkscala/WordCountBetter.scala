package sparkscala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}

object WordCountBetter extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "WordCount")
  val lines = sc.textFile("./data/book.txt")
  val words = lines.flatMap(_.split("\\W+").map(_.toLowerCase))

  val wordCounts = words.countByValue()

  wordCounts.foreach(println)
}

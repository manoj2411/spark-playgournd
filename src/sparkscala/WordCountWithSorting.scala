package sparkscala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object WordCountWithSorting extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local", "WordCountWithSorting")
  val data = sc.textFile("./data/book.txt")
  val words = data.flatMap(_.split("\\W+").map(_.toLowerCase))

  // Adding custom word count so that we return RDD and do sorting on that instead of scala map
  // Since we are doing operation on RDD, spark manages the task distribution and all.
  val wordCounts = words.map((_, 1)).reduceByKey((x, y) => x + y )
//  val result = wordCounts.map(x => (x._2, x._1)).sortByKey()
  val result = wordCounts.sortBy(_._2, ascending = false)
  result.foreach(x => println(s"${x._1}: ${x._2}") )

}

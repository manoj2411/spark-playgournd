package sparkscala

import org.apache.spark._;
import org.apache.spark.SparkContext._;
import org.apache.log4j._;

object PopularMovies extends App {
  Logger.getLogger("org").setLevel(Level.ERROR);

  def parseLine(line: String) = {
    val fields = line.split("\t")
    (fields(1))
  }

  val sc = new SparkContext("local[*]", "PopularMovies")

  val lines = sc.textFile("./data/u.data");

  val movieIds = lines.map(parseLine)

  // Map: (movieId, 1)
  val moviesPair = movieIds.map(x => (x, 1))

  // count all 1's for each movie
  val moviesCount = moviesPair.reduceByKey(_ + _)

  // sort movie map with movie count
  val moviesSorted = moviesCount.sortBy(_._2, ascending = false)

  // starts processing and take 1 record
  val result = moviesSorted.take(1)

  // printing the result
  result.foreach(println)

}

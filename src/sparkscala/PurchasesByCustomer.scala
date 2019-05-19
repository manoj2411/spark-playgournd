package sparkscala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object PurchasesByCustomer extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def parseLine(line: String) = {
    val fields = line.split(",")
    val userId = fields(0).toInt
    val productId = fields(1).toInt
    val amount = fields(2).toDouble
    (userId, productId, amount)
  }

  val sc = new SparkContext("local[*]" ,"PurchasesByCustomer")
  val lines = sc.textFile("./data/customer-orders.csv")
  val rdd = lines.map(parseLine)
  val customerPurchases = rdd.map(x => (x._1, x._3)).reduceByKey(_ + _).collect
  customerPurchases.foreach(println)
}

package com.snist.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up total amt by customer */
object TotalAmtSpentByCust {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val custId = fields(0).toInt
    val amt = fields(2).toFloat
    (custId, amt)
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "CustAmt")

    // Load each line of customer txn into an RDD
   
    val input = sc.textFile("F:/SrinidhiProjects/SparkScala/SparkScala/customer-orders.csv");

    val custTxn = input.map(parseLine)

    val res = custTxn.reduceByKey((x, y) => (x + y))

    res.foreach(println)

  }

}


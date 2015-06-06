package edu.stthomas.gps.bda.project

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import au.com.bytecode.opencsv.CSVParser

object ReasonsForDelayInEachYear {
  def parseLine(line:String)={
    val csvParser = new CSVParser(',')
    csvParser.parseLine(line)
  }

  def main(args: Array[String]) {

    // Use this for Spark on cluster
    val sparkConf = new SparkConf().setAppName("ReasonsForDelayInEachYear")
    val sc = new SparkContext(sparkConf)

    val outputFolder = args(0)

    val data = sc.textFile("onTimePerformance/On_Time_On_Time_Performance*").map(line => parseLine(line));

    // Calculate percentage (What's the reason for the delay in each year?)
    data.filter(l => l(0) != "Year" && l(56) != "") // Filter CSV header and rows without specific delay reasons
      .map(l => (l(0), (l(43).toDouble,l(56).toDouble,l(57).toDouble,l(58).toDouble,l(59).toDouble,l(60).toDouble))) // (Year) => (DepartureDelay, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))  // Accumulate DelayMinutes
      .mapValues{ case (delay, carrier, weather, nas, security, late) => (carrier / delay, weather / delay, nas / delay, security / delay, late / delay) } // Calculate percentages
      .repartition(1) // Use only one reducer for sorting purposes
      .sortByKey() // Sort
      .map(tuple => "%s,%s,%s,%s,%s,%s".format(tuple._1, tuple._2._1, tuple._2._2, tuple._2._3, tuple._2._4, tuple._2._5)) // Map to a CSV format
      .saveAsTextFile(outputFolder) // Call .toString method on RDD
  }
}

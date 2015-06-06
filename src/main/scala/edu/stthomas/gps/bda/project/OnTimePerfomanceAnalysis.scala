package edu.stthomas.gps.bda.project

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

object OnTimePerfomanceAnalysis {
  def parseLine(line:String)= {
    val csvParser = new CSVParser(',')
    csvParser.parseLine(line)
  }

  def main(args: Array[String]) {
    // Use this for Spark on cluster
    val sparkConf = new SparkConf().setAppName("OnTimePerfomanceAnalysis")
    val sc = new SparkContext(sparkConf)

    val departureState = args(0)
    val arrivalState = args(1)
    val outputFolder = args(2)

    val data = sc.textFile("onTimePerformance/On_Time_On_Time_Performance*").map(line => parseLine(line))

    //val airportMapping = sc.textFile("L_AIRPORT_ID.csv").map(line => parseLine(line))
    //val airportMappingBC = sc.broadcast(airportMapping)

    val result = data.filter(line => line(0) != "Year" && line(43) != "" && line(16) == departureState && line(25) == arrivalState) // Filter all rows for departure and arrival state, missing data etc.
      .map(line => ((line(11), line(20), line(4)), (line(43).toDouble, 1))) // Reduce to needed columns
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // Calculate Sum and Count
      .mapValues{ case (sum, count) => (1.0 * sum)/count} // Calculate average
      .map(line => line.swap) // Swap key and value
      // works only in spark-shell (?) .mapValues { case (airportFrom, airportTo, dayOfWeek) => (airportMappingBC.value.filter(airport => airport(0) == airportFrom).first, airportMappingBC.value.filter(airport => airport(0) == airportTo).first, dayOfWeek)}
      .repartition(1) // Use only one reducer for sorting purposes
      .sortByKey() // Sort
      .map(tuple => "%s,%s,%s,%s".format(tuple._1, tuple._2._1, tuple._2._2, tuple._2._3)) // Map to a CSV format
      .saveAsTextFile(outputFolder) // Call .toString method on RDD
  }
}

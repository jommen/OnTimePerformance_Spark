package edu.stthomas.gps.bda.project


import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

object AirportPagerankCentrality {
  def parseLine(line:String)={
    val csvParser = new CSVParser(',')
    csvParser.parseLine(line)
  }

  def main(args: Array[String]) {
    // Use this for Spark on cluster
    val sparkConf = new SparkConf().setAppName("AirportPagerankCentrality")
    val sc = new SparkContext(sparkConf)

    for (year <- 2003 to 2014) {
      val numberOfFlightsBetweenAirports = sc.parallelize(
        sc.textFile("onTimePerformance/On_Time_On_Time_Performance_" + year.toString + "*").filter(line => !line.startsWith("\"Year\"")).map(line => parseLine(line)) // Filter yearly data and map to an array format
          .map(line => ((line(11).toLong, line(20).toLong), 1)) // map to the format (FromAirport, ToAirport) => 1
          .countByKey() // calculate edge weight
          .map { case ((from, to), value) => Edge(from, to, value)} // create Edge instances to generate the graph based on the edges
          .toSeq // we need a sequence
      )
      val graph: Graph[Int, Long] = Graph.fromEdges(numberOfFlightsBetweenAirports, defaultValue = 1)
      graph.pageRank(0.01) // run PageRank algorithm of the GraphX framework
        .vertices // get the pagerank values
        .map(vertex => vertex.swap) // swap the key and value
        .repartition(1) // Use 1 reducer for sorting purposes
        .sortByKey(false) // Sort by PageRank in descending order
        .map(tuple => "%s,%s,%s".format(year, tuple._1, tuple._2)) // Convert into a CSV format
        .saveAsTextFile("AirportPagerankCentrality" + year.toString) // And save
    }
  }
}
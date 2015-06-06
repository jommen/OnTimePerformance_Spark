name := "OnTimePerformance"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.0.0",
  "org.apache.spark" %% "spark-graphx" % "1.0.0",
  "org.apache.spark" %% "spark-mllib" % "1.0.0",
  "net.sf.opencsv" % "opencsv" % "2.3"
)

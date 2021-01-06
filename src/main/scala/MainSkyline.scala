package com.avgitidis.spark

import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.ArrayBuffer

object MainSkyline {


  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val skylines = ArrayBuffer[Row]()
    val kDomSkylines = ArrayBuffer[Row]()
    val nodesBuffer = ArrayBuffer[Row]()
    val kDominant = ArrayBuffer[Row]()
    //    val k: Int = args(0).toInt
    val k: Int = 10
    val cols = List("Nodes Dominating", "A", "B", "C", "D")
    val sep = (1 to 50).map(_ => "_").mkString("_")
    val start = System.nanoTime

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MySkyline")
      .config("spark.executor.cores", 1)
      .config("spark.scheduler.pool", "default")
      .config("spark.scheduler.pool.schedulingMode", "FAIR")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.scheduler.revive.interval", "0s")
      .config("spark.task.cpus", 2)
      .config("spark.reducer.maxSizeInFlight", "400m")
      //            .config("spark.executor.instances", 16)
      .config("spark.dynamicAllocation.enabled", "true")
      //      .config("spark.cores.max", 8)
      .master("local[*]")
      .getOrCreate()

    val pointsSchema = new StructType()
      .add("A", DoubleType, nullable = true)
      .add("B", DoubleType, nullable = true)
      .add("C", DoubleType, nullable = true)
      .add("D", DoubleType, nullable = true)
    //      .add("E", DoubleType, nullable = true)
    //      .add("F", DoubleType, nullable = true)
    //      .add("G", DoubleType, nullable = true)
    //      .add("H", DoubleType, nullable = true)
    //      .add("I", DoubleType, nullable = true)
    //      .add("J", DoubleType, nullable = true)
    //    val filePath = "yourFilePath"

    import spark.implicits._

    println(sep)
    println("Reading the csv file")

    val nodes = spark.read
      .option("header", "true")
      .schema(pointsSchema)
      //      .csv("data/1Ms_10Fdata.csv")
      .csv("data/10K_4Fdata.csv")
      //      .csv(filePath)
      .withColumn("Sum", SkylineFunctions.getSumUDF(col("A"), col("B"), col("C"), col("D")))
      //      .withColumn("Sum",getSumUDF(col("A"),col("B"),col("C"),col("D"),col("E"),col("F"),col("G"),col("H"),col("I"),col("J")))
      .sort($"Sum")
      .drop($"Sum")
      .cache()

    //  Task 1
    val t1 = System.nanoTime
    println("Starting Task1")
    println("Calculating Skyline Points")

    nodes.collect.foreach(x => SkylineFunctions.sfsSkyline(skylines, Iterator(x))
    )

    println("Found " + skylines.length + " Skyline Points")
    println("Task1 finished in: " + BigDecimal((System.nanoTime - t1) / 1e9d).setScale(2, BigDecimal.RoundingMode.HALF_DOWN).toFloat + " seconds")
    println(sep)
    //       skylines.foreach(f=>{
    //           println(f)
    //       })

    //Task 2
    val t2 = System.nanoTime
    println("Starting Task2")

    nodes.collect.foreach(x => SkylineFunctions.dfToIterator(nodesBuffer, Iterator(x))
    )
    nodes.collect.foreach(x => SkylineFunctions.kDominateCount(kDominant, nodesBuffer, Iterator(x))
    )
    val kDf2 = kDominant.groupBy(identity).mapValues(_.size).toList.map(_.swap)
      .map({
        case (x: Int, Row(val1: Double, val2: Double, val3: Double, val4: Double)) => (x, val1, val2, val3, val4)
      }
      )
      .toDF(cols: _*)
      .sort(desc("Nodes Dominating"))

    println("The " + k + " Most Dominant Nodes")
    kDf2.show(k)
    println("Task2 finished in: " + BigDecimal((System.nanoTime - t2) / 1e9d).setScale(2, BigDecimal.RoundingMode.HALF_DOWN).toFloat + " seconds")
    println(sep)

    // Task 3
    val t3 = System.nanoTime
    println("Starting Task3")

    nodes.collect.foreach(x => SkylineFunctions.kDominateCount(kDomSkylines, skylines, Iterator(x))
    )

    val kDf = kDomSkylines.groupBy(identity).mapValues(_.size).toList.map(_.swap)
      .map({
        case (x: Int, Row(val1: Double, val2: Double, val3: Double, val4: Double)) => (x, val1, val2, val3, val4)
      }
      )
      .toDF(cols: _*)
      .sort(desc("Nodes Dominating"))
    println("The " + k + " Most Dominant Skyline Nodes")
    kDf.show(k)
    println("Task3 finished in: " + BigDecimal((System.nanoTime - t3) / 1e9d).setScale(2, BigDecimal.RoundingMode.HALF_DOWN).toFloat + " seconds")
    println(sep)

    spark.stop()
    println("All requested tasks finished in " + BigDecimal((System.nanoTime - start) / 1e9d).setScale(2, BigDecimal.RoundingMode.HALF_DOWN).toFloat + " seconds")
    println(sep)
  }
}





package com.avgitidis.spark

import org.apache.log4j._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object MainSkyline {


  /** Our main function where the action happens */
  def main(args: Array[String]) {

    //    val kDomSkylines = ArrayBuffer[Row]()
    //    val nodesBuffer = ArrayBuffer[Row]()
    //    val kDominant = ArrayBuffer[Row]()

    //We can implement an args parser in case we compile it into a jar
    val k: Int = 10
    val dims: Int = 10
    val size: String = "10K"
    val distribution: String = "chisquare"

    val domscore: String = "DominanceScore"
    val countstr: String = "(count - 1)"
    var skylines = ArrayBuffer[Row]()
    val cols = List("A", "B", "C", "D", "E", "F", "G", "H", "I", "J")
    val columns = cols.slice(0, dims)
    val cols_ = columns :+ "Id"
    val sep = (1 to 50).map(_ => "_").mkString("_")
    val start = System.nanoTime

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MySkyline")
      .config("spark.scheduler.pool", "default")
      .config("spark.scheduler.pool.schedulingMode", "FAIR")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.scheduler.revive.interval", "0s")
      .config("spark.reducer.maxSizeInFlight", "400m")
      .config("spark.executor.memory", "24g")
      .config("spark.dynamicAllocation.enabled", "true")
      //      .config("spark.task.cpus", 2)
      .master("local[*]")
      .getOrCreate()

    val nodeSchema = StructType(columns.map(n => StructField(n, DoubleType, nullable = true)))
    val encoder = RowEncoder(nodeSchema)
    import spark.implicits._
    println(sep)
    println("Reading the csv file")

    val nodes = spark.read
      .option("header", "true")
      .schema(nodeSchema)
      .csv(s"data/${size}x${dims}_$distribution.csv")
      .withColumn("Sum", SkylineFunctions.getSumUDF(struct(columns.map(col): _*)))
      .withColumn("Id", monotonically_increasing_id())
      .sort($"Sum")
      .drop($"Sum")
      .as("nodes")
      .toDF
      .cache()

    //  Task 1
    val t1 = System.nanoTime
    println("Starting Task1")
    println("Calculating Skyline Points")


    nodes.collect.par.foreach(x => SkylineFunctions.sfsSkyline(skylines, Iterator(x)))
    nodes.collect.par.foreach(x => SkylineFunctions.onepctSkylines(skylines, Iterator(x)))
    skylines = skylines.distinct

    println("Found " + skylines.length + " Skyline Points")
    val t1_ = BigDecimal((System.nanoTime - t1) / 1e9d).setScale(2, BigDecimal.RoundingMode.HALF_DOWN).toDouble
    println("Task1 finished in: " + t1_ + " seconds")

    //           skylines.foreach(f=>{
    //               println(f)
    //           })
    println(sep)

    //     Task 3
    val t3 = System.nanoTime
    println("Starting Task3")

    var skylinesdf = spark.emptyDataFrame
    dims match {
      case 2 =>
        skylinesdf = skylines.par
          .map({
            case Row(val1: Double, val2: Double, id: Long) => (val1, val2, id)
          }
          )
          .toList
          .toDF(cols_ : _*)
          .as("skylines")
      case 5 =>
        skylinesdf = skylines.par
          .map({
            case Row(val1: Double, val2: Double, val3: Double, val4: Double, val5: Double, id: Long) => (val1, val2, val3, val4, val5, id)
          }
          )
          .toList
          .toDF(cols_ : _*)
          .as("skylines")
      case 10 =>
        skylinesdf = skylines.par
          .map({
            case Row(val1: Double, val2: Double, val3: Double, val4: Double, val5: Double, val6: Double, val7: Double, val8: Double, val9: Double, val10: Double, id: Long) => (val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, id)
          }
          )
          .toList
          .toDF(cols_ : _*)
          .as("skylines")
    }

    val jdf = nodes.crossJoin(skylinesdf)
    var kdom_skyline = spark.emptyDataFrame

    dims match {
      case 2 =>
        kdom_skyline = jdf.filter(col("skylines.A") <= col("nodes.A") &&
          col("skylines.B") <= col("nodes.B"))
          .groupBy("skylines.Id")
          .count()
          .select($"id", $"count" - 1).withColumnRenamed(countstr, domscore)
          .as("kskylines")
          .join(skylinesdf, $"skylines.Id" === $"kskylines.Id")
          .drop("Id")
          .sort(desc(domscore))
      case 5 =>
        kdom_skyline = jdf.filter(col("skylines.A") <= col("nodes.A") &&
          col("skylines.B") <= col("nodes.B") &&
          col("skylines.C") <= col("nodes.C") &&
          col("skylines.D") <= col("nodes.D") &&
          col("skylines.E") <= col("nodes.E"))
          .groupBy("skylines.Id")
          .count()
          .select($"id", $"count" - 1).withColumnRenamed(countstr, domscore)
          .as("kskylines")
          .join(skylinesdf, $"skylines.Id" === $"kskylines.Id")
          .drop("Id")
          .sort(desc(domscore))
      case 10 =>
        kdom_skyline = jdf.filter(col("skylines.A") <= col("nodes.A") &&
          col("skylines.B") <= col("nodes.B") &&
          col("skylines.C") <= col("nodes.C") &&
          col("skylines.D") <= col("nodes.D") &&
          col("skylines.E") <= col("nodes.E") &&
          col("skylines.F") <= col("nodes.F") &&
          col("skylines.G") <= col("nodes.G") &&
          col("skylines.H") <= col("nodes.H") &&
          col("skylines.I") <= col("nodes.I") &&
          col("skylines.J") <= col("nodes.J"))
          .groupBy("skylines.Id")
          .count()
          .select($"id", $"count" - 1).withColumnRenamed(countstr, domscore)
          .as("kskylines")
          .join(skylinesdf, $"skylines.Id" === $"kskylines.Id")
          .drop("Id")
          .sort(desc(domscore))
    }

    kdom_skyline.show(k)
    //
    //    nodes.collect.foreach(x => SkylineFunctions.kDominateCount(kDomSkylines, skylines, Iterator(x))
    //    )
    //
    //    val kdf_ = kDomSkylines.groupBy(identity).mapValues(_.size).par.map(_.swap)
    //      .map({
    //        case (x: Int, Row(val1: Double, val2: Double,id:Long)) => (x, val1, val2,id)
    //            case Row(val1: Double, val2: Double, val3: Double, val4: Double,val5: Double,id:Long) => (val1, val2, val3, val4, val5,id)
    //        case (x: Int, Row(val1: Double, val2: Double, val3: Double, val4: Double, val5: Double, val6: Double, val7: Double, val8: Double, val9: Double, val10: Double,id:Long)) => (x, val1, val2, val3, val4, val5,val6,val7,val8,val9,val10,id)
    //      }
    //      )
    //      .toList
    //      .toDF(cols: _*)
    //      .sort(desc("Nodes Dominating"))
    //    println("The " + k + " Most Dominant Skyline Nodes")
    //    kdf_.show(k)
    val t3_ = BigDecimal((System.nanoTime - t3) / 1e9d).setScale(2, BigDecimal.RoundingMode.HALF_DOWN).toDouble
    println("Task3 finished in: " + t3_ + " seconds")
    println(sep)

    //Task 2
    val t2 = System.nanoTime
    println("Starting Task2")

    val jdf2 = nodes.as("nodes").crossJoin(nodes.as("nodes1")).join(skylinesdf, $"nodes1.Id" =!= "skylines.Id", "left_anti")
    jdf2.head()
    var kdom_all = spark.emptyDataFrame

    dims match {
      case 2 =>
        kdom_all = jdf2.filter(col("nodes.A") <= col("nodes1.A") &&
          col("nodes.B") <= col("nodes1.B"))
          .groupBy("nodes.Id")
          .count()
          .select($"id", $"count" - 1).withColumnRenamed(countstr, domscore)
          .as("fnodes")
          .join(nodes, $"fnodes.Id" === $"nodes.Id")
          .drop("Id")
          .sort(desc(domscore))
      case 5 =>
        kdom_all = jdf2.filter(col("nodes.A") <= col("nodes1.A") &&
          col("nodes.B") <= col("nodes1.B") &&
          col("nodes.C") <= col("nodes1.C") &&
          col("nodes.D") <= col("nodes1.D") &&
          col("nodes.E") <= col("nodes1.E"))
          .groupBy("nodes.Id")
          .count()
          .select($"id", $"count" - 1).withColumnRenamed(countstr, domscore)
          .as("fnodes")
          .join(nodes, $"fnodes.Id" === $"nodes.Id")
          .drop("Id")
          .sort(desc(domscore))
      case 10 =>
        kdom_all = jdf2.filter(col("nodes.A") <= col("nodes1.A") &&
          col("nodes.B") <= col("nodes1.B") &&
          col("nodes.C") <= col("nodes1.C") &&
          col("nodes.D") <= col("nodes1.D") &&
          col("nodes.E") <= col("nodes1.E") &&
          col("nodes.F") <= col("nodes1.F") &&
          col("nodes.G") <= col("nodes1.G") &&
          col("nodes.H") <= col("nodes1.H") &&
          col("nodes.I") <= col("nodes1.I") &&
          col("nodes.J") <= col("nodes1.J"))
          .groupBy("nodes.Id")
          .count()
          .select($"id", $"count" - 1).withColumnRenamed(countstr, domscore)
          .as("fnodes")
          .join(nodes, $"fnodes.Id" === $"nodes.Id")
          .drop("Id")
          .sort(desc(domscore))
    }

    kdom_all.union(skylinesdf).sort(desc(domscore))
    kdom_all.show(k)

    //    nodes.collect.foreach(x => SkylineFunctions.dfToIterator(nodesBuffer, Iterator(x))
    //    )
    //    nodes.collect.foreach(x => SkylineFunctions.kDominateCount(kDominant, nodesBuffer, Iterator(x))
    //    )
    //    val kdf = kDominant.groupBy(identity).mapValues(_.size).par.map(_.swap)
    //      .map({
    //            case (x: Int, Row(val1: Double, val2: Double,id:Long)) => (x, val1, val2,id)
    //            case Row(val1: Double, val2: Double, val3: Double, val4: Double,val5: Double,id:Long) => (val1, val2, val3, val4, val5,id)
    //            case (x: Int, Row(val1: Double, val2: Double, val3: Double, val4: Double, val5: Double, val6: Double, val7: Double, val8: Double, val9: Double, val10: Double,id:Long)) => (x, val1, val2, val3, val4, val5,val6,val7,val8,val9,val10,id)
    //      }
    //      )
    //      .toList
    //      .toDF(cols: _*)
    //      .sort(desc("Nodes Dominating"))
    //
    //    println("The " + k + " Most Dominant Nodes")
    //    kdf.show(k)
    val t2_ = BigDecimal((System.nanoTime - t2) / 1e9d).setScale(2, BigDecimal.RoundingMode.HALF_DOWN).toDouble
    println("Task2 finished in: " + t2_ + " seconds")
    println(sep)


    spark.stop()
    val duration = BigDecimal((System.nanoTime - start) / 1e9d).setScale(2, BigDecimal.RoundingMode.HALF_DOWN).toDouble
    println("All requested tasks finished in " + duration + " seconds")
    println(sep)
    //    println(s"$duration $t1_ $t2_ $t3_")
  }
}
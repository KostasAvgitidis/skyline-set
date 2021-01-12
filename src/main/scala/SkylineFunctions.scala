package com.avgitidis.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object SkylineFunctions {
  //Simple Sum UDF
  val getSum: Row => Double = (row: Row) => {
    var sum: Double = 0
    val rows = row.toSeq
    for (i <- rows.indices) {
      sum += rows(i).toString.toDouble
    }
    sum
  }
  val getSumUDF: UserDefinedFunction = udf(getSum)

  //Sort Filter Skyline Algorithm
  def sfsSkyline(skylines: ArrayBuffer[Row], rowIterator: Iterator[Row]): Iterator[Row] = {
    var initial = false
    val row = rowIterator.toList.head
    if (skylines.isEmpty) {
      skylines += row
      initial = true
    }
    var j = 0
    var stop = false
    breakable {
      while (skylines.length > j) {
        try {
          if (aDominatesb(row, skylines(j))) {
            skylines -= skylines(j)
            stop = false
            j -= 1
          }
          else if (aDominatesb(skylines(j), row)) {
            stop = true
            break()
          }
        }
        catch {
          case e: IndexOutOfBoundsException =>
            stop = true
            break()
        }
        if (initial) {
          stop = true
          break()
        }
        j += 1
      }
      if (!stop) {
        skylines += row
      }
    }
    rowIterator
  }

  //Point A -Dominates-> Point B
  def aDominatesb(x: Row, y: Row): Boolean = {
    try {
      val features = x.length
      for (i <- 0 until features - 1) {
        if (x(i).toString.toDouble > y(i).toString.toDouble) {
          return false
        }
      }
      for (j <- 0 until features - 1) {
        if (x(j).toString.toDouble < y(j).toString.toDouble) {
          return true
        }
      }
    }
    catch {
      case e: NullPointerException => return true
    }
    false
  }

  //0.1% Skylines left
  def onepctSkylines(skylines: ArrayBuffer[Row], rowIterator: Iterator[Row]): Iterator[Row] = {
    val row = rowIterator.toList.head
    var j = 0
    var stop = false
    breakable {
      while (skylines.length > j) {
        if (aDominatesb(row, skylines(j))) {
          skylines -= skylines(j)
          stop = false
          j -= 1
        }
        else if (aDominatesb(skylines(j), row)) {
          stop = true
          break()
        }
        j += 1
      }
    }
    if (!stop) {
      skylines += row
    }
    rowIterator
  }

  //Converting a Dataframe into an Iterator[Row] (Task 3 alternative version)
  def dfToIterator(buffer: ArrayBuffer[Row], rowIterator: Iterator[Row]): Iterator[Row] = {
    val row = rowIterator.toList.head
    buffer += row
    buffer.toIterator
  }

  //Get k-Dominating points (Task2 and 3 alternative version)
  def kDominateCount(buffer: ArrayBuffer[Row], skylines: ArrayBuffer[Row], it: Iterator[Row]): ArrayBuffer[Row] = {
    val row = it.toList.head
    for (j <- skylines.indices) {
      if (aDominatesb(skylines(j), row)) {
        buffer += skylines(j)
      }
    }
    buffer
  }
}
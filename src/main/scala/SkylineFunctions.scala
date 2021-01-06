package com.avgitidis.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object SkylineFunctions {
  //Simple Sum UDF
  val getSum: (Float, Float, Float, Float) => Float = (a: Float, b: Float, c: Float, d: Float) => {
    //  val getSum : (Float,Float,Float,Float,Float,Float,Float,Float,Float,Float) => Float = (a:Float,b:Float,c:Float,d:Float,e:Float,f:Float,g:Float,h:Float,i:Float,j:Float)=>{
    //    a + b + c + d + e + f + g + h + i + j
    a + b + c + d
  }

  val getSumUDF: UserDefinedFunction = udf(getSum)

  //Point A -Dominates-> Point B
  def aDominatesb(x: Row, y: Row): Boolean = {
    val features = x.length
    for (i <- 0 until features) {
      if (x(i).toString.toDouble > y(i).toString.toDouble) {
        return false
      }
    }
    for (j <- 0 until features) {
      if (x(j).toString.toDouble < y(j).toString.toDouble) {
        return true
      }
    }
    false
  }

  //Sort Filter Skyline Algorithm
  def sfsSkyline(skylines: ArrayBuffer[Row], rowIterator: Iterator[Row]): Iterator[Row] = {
    var initial = false
    val rowArray = rowIterator.toArray
    if (skylines.isEmpty) {
      skylines += rowArray(0)
      initial = true
    }
    for (i <- rowArray.indices) {
      var j = 0
      var stop = false
      breakable {
        while (j < skylines.length) {

          if (aDominatesb(rowArray(i), skylines(j))) {
            skylines.remove(j)
            j -= 1
          }
          else if (aDominatesb(skylines(j), rowArray(i))) {
            stop = true
            break()
          }
          if (initial & i == 0) {
            stop = true
            break()
          }
          j += 1
        }
      }
      if (!stop) {
        skylines += rowArray(i)
      }
    }
    skylines.toIterator
  }

  //Converting a Dataframe into an Iterator[Row]
  def dfToIterator(buffer: ArrayBuffer[Row], rowIterator: Iterator[Row]): Iterator[Row] = {
    val row = rowIterator.toArray
    buffer += row(0)
    buffer.toIterator
  }

  //Get k-Dominating points
  def kDominateCount(buffer: ArrayBuffer[Row], skylines: ArrayBuffer[Row], it: Iterator[Row]): ArrayBuffer[Row] = {
    val rowArray = it.toArray
    for (i <- rowArray.indices) {
      for (j <- skylines.indices) {
        if (aDominatesb(skylines(j), rowArray(i))) {
          buffer += skylines(j)
        }
      }
    }
    buffer
  }
}
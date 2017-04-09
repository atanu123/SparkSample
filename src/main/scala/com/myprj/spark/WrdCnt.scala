package com.myprj.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WrdCnt {
  
 def print(data:String) = {
      println(data)
  }
      
  def main(args: Array[String]) = {

    println("**** S T A R T ******")
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")

    val sc = new SparkContext(conf)

    //Read some example file to a test RDD
    val test = sc.textFile("input.txt")
    
    print( "**** Flatmap Starting ******")
    //for each line split the line in word by word.
    val tmplines = test.flatMap { line => line.split(" ") }
    tmplines.collect().foreach(println)

    print("**** Map Starting ******")
    //for each word Return a key/value tuple, with the word as key and 1 as value
    val keywords = tmplines.map { word => (word, 1) }
    keywords.collect().foreach(println)

    print("**** Reduce by Key Starting ******")
    //Sum all of the value with same key
    val sumwords = keywords.reduceByKey(_ + _)
    sumwords.collect().foreach(println)

    //Stop the Spark context
    print("**** S T O P ******")
    sc.stop
  }
}
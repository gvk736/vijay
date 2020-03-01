package com.practice.sparkandscala

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\wordcountassngmnt.txt")
    val word = data.flatMap(x => x.split(" "))//.map(x => (x, 1)).reduceByKey(_ + _)
    //println(word.collect().toList)
    println(word.count())



  }
}
package com.practice.sparkandscala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._


object transformsandactions1 extends App {

  //val conf = new SparkConf()
  //val sc = new SparkContext(conf)
 // val sqlctx = new SQLContext(sc)
  //sc.setLogLevel("ERROR")

  val spark = SparkSession.builder().appName("transforms").master("local").getOrCreate()

  val Df1 = spark.read.option("header","true").option("sep","\t").csv("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\aicte-ece-staff.txt")
  //Df1.printSchema()
  //Df1.show()

  import spark.implicits._

//val Df2 = Df1.createOrReplaceTempView("vijay")
  //val Df3 = Df1.select("Name","Date of Birth","Gender").withColumn("year", Df1)
  val Df3 = Df1.filter($"Date of Birth"==="07-03-1986").show()


}

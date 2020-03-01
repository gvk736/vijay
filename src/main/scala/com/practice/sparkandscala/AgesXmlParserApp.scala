package com.practice.sparkandscala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AgesXmlParserApp extends App {

  val spark = SparkSession.builder().appName("xml file").master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")


  val xmlDF = spark.read.format("xml").option("rowTag", "people").load("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\ages.xml")
  xmlDF.printSchema()

//  xmlDF.select(explode(col("person")).as("person"))

  xmlDF.show()

  // import sqlContext.implicits._
  //val df1 = xmlDF.select(xmlDF("name").alias("name"),
  //$"age._VALUE".as("actualage"),
  //col("age._born").alias("birthdate"))
  //df1.show

  //val groupedDF = df1.groupBy(col("actualage")).count()
  // groupedDF.show


}

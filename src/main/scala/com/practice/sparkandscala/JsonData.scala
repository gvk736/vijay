import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object JsonData extends App {

  val spark = SparkSession.builder().appName("AICTE DATA").master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")


  import spark.implicits._
  import  org.apache.spark.sql.types._

  /*
    val userSchema = StructType(List(StructField("Year", StringType, false),StructField("First Name", StringType, false),StructField("County", StringType, false),StructField("Sex", StringType, false),StructField("Count", StringType, false )))
    val userDF = spark.read.option("multiline", "true").schema(userSchema).json("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\employee.json")

  userDF.show()

  */


  /*
    val df = spark.read.json("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\Model.json")

    val df2 = df.withColumn("lang", explode($"lang")).withColumn("id",$"lang"(0)).withColumn("language",$"lang"(1)).withColumn("Description",$"lang"(2)).drop("lang")
  package com.practice.sparkandscala

   // val df3=df2.select(col("id"),col("language"),col("Description"))
    df2.show()
  */
/*
  val readJSON = sc.wholeTextFiles("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\Model.json")
    .map(x => x._2)
    .map(data => data.replaceAll("\n", ""))

 //val df=readJSON.toDF()
  //df.show()
  val df = spark.read.json(readJSON)
  val df2 = df.withColumn("lang", explode($"lang"))
    .withColumn("id",$"lang"(0)).withColumn("language",$"lang"(1))
    .withColumn("Description",$"lang"(2)).drop("lang")

  df.show()
*/

  val JsonRD = sc.wholeTextFiles("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\sample.json").map(data=> data._2).map(data=>data.replaceAll("\n", ""))

  val df=spark.read.json(JsonRD)
  val df2=df.select(explode($"content")).toDF("CONTENT")
  val df3=df2.select("CONTENT.foo","CONTENT.bar")
  df3.show()

}

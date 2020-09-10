package com.practice.sparkandscala

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

object Expld {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local").appName("explode function").getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    val df1 = spark.read.option("header","true").csv("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\explde.csv")
   df1.createOrReplaceTempView("expd")
val df2 =spark.sql("select * from expd").show()

  }

}

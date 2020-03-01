package com.practice.sparkandscala


import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf

/**
  * Created by Vijay Krishna on 06-08-2019.
  */
object sparkcoreread extends App {

  val spark = SparkSession.builder().appName("AICTE DATA").master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")


  import spark.implicits._

  def dateFn(col:String): String ={
    val d=col.replaceAll("-","")
    val e=d.substring(4,8)
    return e
  }

  val date=udf(dateFn _)

  val Df1 = spark.read.option("header", "true").option("sep", "\t").csv("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\aicte-ece-staff.txt")
 // Df1.printSchema()
  //Df1.show()

  val DF2=Df1.select(col = ("Name"),("Gender"),("Religion"),("Date of Birth")).withColumn("year",date($"Date of Birth"))

  DF2.show()



}



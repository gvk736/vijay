package com.practice.sparkandscala

import org.apache.spark.sql.{SparkSession, functions}

object TableCreation extends  App {


  val spark = SparkSession.builder().appName("AICTE DATA").master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")


  import spark.implicits._


  val TableDF=spark.read.option("header", "true").option("sep", "\t").csv("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\aicte-ece-staff.txt")
//TableDF.show()


  TableDF.createOrReplaceTempView("Tableaicte")


  val DF1=spark.sql("select * from Tableaicte where Caste is OBC ").show()
 // val DF1=TableDF.show()

  //val DF1=TableDF.withColumn("DOB", functions.regexp_replace(TableDF.col("Date of Birth"), "\\p{Blanck}", ""))
  //val DF2=TableDF.withColumnRenamed("Date of Birth","DOB")


// DF1.show()
 /*
  TableDF.write.mode("overwrite").option("header", true)
    .option("sep", "\t")
    .option("inferSchema", false)
    .option("charset", "UTF-8")
    .csv("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\TableAicte")


    //save("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\aicteTable")
*/
}




package com.practice.sparkandscala

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object textbookexmpls1 extends App {
  val conf = new SparkConf().setAppName("vijay").setMaster("local")
  val sc = new SparkContext(conf)
//val sqlContext=new SQLContext(sc)
  val spark = SparkSession.builder().appName("page 118 DF API").master("local").getOrCreate()
/*
  case class Customer(cId: Long,name:String,age:Int,gender:String)
  val customers= List(Customer(1,"James",21,"M"),Customer(2,"Liz",25,"F"),Customer(3,"John",31,"M"),Customer(4,"Jennifer",45,"F")
  ,Customer(5,"Robert",41,"M"),Customer(6,"Sandra",45,"F"))
     */


  import spark.implicits._
  val customerDF = spark.read.option("header","true").option("sep","\t").csv("C:\\Users\\Vijay Krishna\\IdeaProjects" +
    "\\vijaypractice\\src\\main\\resources\\aicte-ece-staff.txt").toDF()
//customerDF.printSchema()
  //customerDF.show()
//val cols = customerDF.columns
  //cols.foreach(println)
  //val colsWithTypes = customerDF.dtypes
  //colsWithTypes.foreach(println)
  //customerDF.explain()
/*
  val dt = System.currentTimeMillis()
  val curDate = new Date(dt)
  val dtFormat = new SimpleDateFormat("DD-MM-YYYY")
  val dobToAge = udf((dob:String)=> {
    val ptDate = dtFormat.parse(dob)
    val ntDate = new Date(ptDate.getTime)
    curDate.getYear - ntDate.getYear
  })

  val Df2 =customerDF.withColumnRenamed("Date of Birth","DOB").withColumn("age",dobToAge($"DOB")).select("Name","DOB","age")
Df2.show()
  */
// val Df2= customerDF.filter($"Caste"==="  " && $"PAN"===" " &&$"Gender"==="Male || Female").select("Name","Caste","PAN","Gender","Date of Birth")
val Df2 = customerDF.rollup($"Name",$"Caste",$"PAN",$"Gender").count()//filter($"Caste"==="null" && $"PAN"==="null").select("Name","Caste","PAN","Gender")
  Df2.show()
}

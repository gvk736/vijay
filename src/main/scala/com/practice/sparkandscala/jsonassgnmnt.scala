package com.practice.sparkandscala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object jsonassgnmnt extends App {

  val spark = SparkSession.builder().appName("json files load").master("local").getOrCreate()
  val sc =spark.sparkContext
  sc.setLogLevel("ERROR")

import spark.implicits._
  val dept =spark.read.json("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\deptdata.json")

 // dept.show()
val emp = spark.read.json("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\empdata.json")
//emp.show()

  val joinjson = dept.join(emp,$"emp_id" === $"id").select("emp_id","name","dept_name")
  joinjson.show()
}

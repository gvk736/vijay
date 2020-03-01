package com.practice.sparkandscala

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object practicesparksql1 extends App {

  val spark = SparkSession.builder().appName("spark SQL Practice").master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  import spark.implicits._

val aicteDf = spark.read.option("header","true").option("sep","\t").csv("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\aicte-ece-staff.txt")
    //aicteDf.show()
//1. cache
  val cache = aicteDf.cache()
//2.columns
  val col= aicteDf.columns
     // col.foreach(println)
//3.dtypes
  val dtps=aicteDf.dtypes
     // dtps.foreach(println)
//4.explain
  val expln=aicteDf.explain()
//  println(expln)
//5.persist
  val prst=aicteDf.persist()
//6.printSchema
  aicteDf.printSchema()
//7. registerTempTable
  aicteDf.createOrReplaceTempView("aicte")
val countDF=spark.sql("select count(1) as cnt from aicte")
  //countDF.show()
//8.toDF
  val resultDF = spark.sql("select count(1) from aicte")
  val countDF1 = resultDF.toDF("cnt")
    //countDF1.show()
//9.agg
  val aggregates = aicteDf.agg(max("Gross Pay per month"),min("Gross Pay per month"),count("name"))
   // aggregates.show()
//10.apply
val aggregates1 = aicteDf.agg(max($"Gross Pay per month"),min($"Gross Pay per month"),count($"name"))
  //aggregates1.show()
//11.cube
  val aicteCubeDf = aicteDf.cube($"Date of Joining",$"Aadhaar Card(UID)",$"Caste")//.sum("Gross Pay per month")

   //aicteCubeDf.withColumnRenamed("sum(Gross Pay per month)","total").show(30)

        //aicteDf.select("Gross Pay per month").show(49)
//12. explode
  val wordDF = aicteDf.explode("Name","design"){Name:String =>Name.split(" ")}
     //wordDF.show()
//13.filter
  val filtDF=aicteDf.filter($"Gross Pay per month">20000)
 // filtDF.show()
//14. groupyBy
  val grp= aicteDf.groupBy("Gender").count()
  //grp.show()
//15.limit
 val lt = aicteDf.limit(6)
  //lt.show()
//16.orderBy
  val ordr=aicteDf.orderBy("Name")
  //ordr.show()
//17.rollup

val rlup = aicteDf.rollup($"Name",$"Gender",$"Caste")
  //rlup.show()
//18. select
  val slct = aicteDf.select($"Name",$"`Gross Pay per month`"+5000)
  //slct.show()
val sql1=spark.sql("select Name,count(Caste) as total from aicte group By Name order By total")
 // sql1.show(30)


  //sql statements
  val count2 = aicteDf.count()
    //println(count2)
val count2e= spark.sql("select count(*) from aicte")
  //count2e.show()
val gndr= aicteDf.groupBy("Gender").count()
  //gndr.show()
val gndr1= spark.sql("select Gender,count(*) from aicte group By Gender")
gndr1.show()
  val cast= aicteDf.groupBy("Caste").count()
  cast.orderBy($"count".desc).show()
  val cast1= spark.sql("select Caste,count(*) from aicte group By Caste order By count desc")
  cast1.show()




  val rank = spark.table("DX_CLM")
  


}

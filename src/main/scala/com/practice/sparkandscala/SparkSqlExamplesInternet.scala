package com.practice.sparkandscala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object SparkSqlExamplesInternet extends App {

  import org.apache.spark.sql.functions.{when, _}
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

import  spark.implicits._
 /*
  import spark.sqlContext.implicits._
  val data = List(("James ","","Smith","36636","M",60000),
    ("Michael ","Rose","","40288","M",70000),
    ("Robert ","","Williams","42114","",400000),
    ("Maria ","Anne","Jones","39192","F",500000),
    ("Jen","Mary","Brown","","F",0))

  val cols = Seq("first_name","middle_name","last_name","dob","gender","salary")
  val df = spark.createDataFrame(data).toDF(cols:_*)

//1. Using “when otherwise” on Spark DataFrame.
  val df2 = df.withColumn("new_gender",when(col("gender")=== "M","Male")
    .when(col("gender")==="F","Female").otherwise("Unknown"))

  //OR  when can also be used on Spark SQL select statement

val df4 = df.select(col("*"),when(col("gender")==="M","Male")
  .when(col("gender")==="F","Female").otherwise("Unknown").alias("new_gender"))

  //2. Using “case when” on Spark DataFrame.
  //Similar to SQL syntax, we could use “case when” with expression expr() .

  val df3 = df.withColumn("new_gender",expr("case when gender = 'M' then 'Male'"+
    "when gender = 'F' then 'Female'"+"else 'Unknown' end"))

  //OR Using SQL Select
val df5 = df.select(col("*"),expr("case when gender = 'M' then 'Male'"+
  "when gender = 'F' then 'Female'"+"else 'Unknown' end").alias("new_gender"))


  val dataDF = Seq((66,"a","4"),(67,"a","0"),(70,"b","4"),(71,"d","4")).toDF("id","code","amt")
  dataDF.withColumn("new_column",when(col("code")=== "a" || col("code")=== "d","A")
  .when(col("code")=== "b" && col("amt")=== "4","B").otherwise("A1")).show()
  //df5.show()
*/

  val df1=spark.read.option("header","true").csv("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\vijay-customers.txt")

  val df2=df1.selectExpr("cast(ID as Int) ID", "cast(NAME as String) NAME","cast(AGE as Int) AGE","cast(ADDRESS as String) ADDRESS","cast(SALARY as Double) SALARY")


/*
  //df2.printSchema()
    df2.createOrReplaceTempView("customers")
 val df3=spark.sql("SELECT AGE, sum(SALARY) FROM customers GROUP BY AGE HAVING sum(salary)>5000.00")

  df3.show()


  val df4=df2.groupBy("age").agg(sum("salary").alias("sumsal")).filter("sumsal>5000.00").select("age","sumsal")

  df4.write.mode("overwrite").format("csv").save("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\tblquery")
  //df4.show()

  */
  val df3=df2.filter($"SALARY" >=3000 && $"SALARY" <= 6000)
  df3.show()

}

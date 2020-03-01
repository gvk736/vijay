package com.practice.sparkandscala

import com.practice.sparkandscala.sparkcoreread.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object socialfriends {


  val spark = SparkSession.builder().appName("socialfrnds").master("local").getOrCreate()
  //  val conf = new SparkConf().setAppName("socialfrnds").setMaster("local").set("spark.driver.allowMultipleContexts", "true")
  //val sc = new SparkContext(conf)
  //val sqlContext = new SQLContext(sc)
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")


  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    // Create a tuple that is our result.
    (age, numFriends)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
   // Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
  //  val sc = new SparkContext("local[*]", "FriendsByAge")

    // Load each line of the source data into an RDD
    val lines = sc.textFile("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\social_friends.csv")

    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)

    // Lots going on here...
    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
    // adding together all the numFriends values and 1's respectively.
    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // So now we have tuples of (age, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age.
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = averagesByAge.collect()

    // Sort and print the final results.
    results.sorted.foreach(println)
  }
}

/*
    import spark.implicits._

   def parseLine(line):
val  fields = line.split(",")
 val age = int(fields[2])
val  numFriends = int(fields[3])
  return(age, numFriends)

val testRDD = sc.textFile("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\social_friends.csv")
   // val testRDD = spark.read.option("header","true").option("delimiter",",").csv("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\social_friends.csv")
   val testRDD1 = testRDD.map(row => row.split(",")).map(x=>(x(2),x(3)))

  val testRDD2=  testRDD1.collect().foreach(println)

  val totalsByAge = testRDD1.mapValues(x=> (x, 1)).reduceByKey((x,y)=>(x._1 + y._1, x._2 + y._2)
*/

package com.practice.sparkandscala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object UserAndTransactions extends App {

  val spark = SparkSession.builder().appName("unique locations of two datas").master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._


    val transactions = sc.textFile("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\transactions.txt")
  transactions.foreach(println)
  val users = sc.textFile("C:\\Users\\Vijay Krishna\\IdeaProjects\\vijaypractice\\src\\main\\resources\\users.txt")
users.foreach(println)
 val newTransactionsPair = transactions.map{t =>{
 val column =  t.split(" ")
 ( column(2), column(1))
  }
}
 newTransactionsPair.foreach(println)

val usersPair = users.map{ele => {
 // val reg = """[\t\p{Zs}]+"""
  //val col = ele.replaceAll(reg, "\t")
  val col = ele.split("\\s+")
  (col(0), col(3))
}
}
  usersPair.foreach(println)
 val grp = newTransactionsPair.join(usersPair)
  grp.foreach(println)
  val dist = grp.map(e => (e._2._2,e._2._1))
  dist.foreach(println)
 // val grp1 = dist.groupByKey()
  //grp1.foreach(println)
 //println( dist.collect())

//  val result1 = result.count()
  //println(result1)
   // val joinRDD = newTransactionsPair.leftOuterJoin(newUsersPair)//.distinct().map(t=>(t._1.toString,t._2.toString))
   //println(joinRDD.collect())

  /*

    val result = processData(newTransactionsPair, newUsersPair)
    return sc.parallelize(result.toSeq).map(t => (t._1.toString, t._2.toString))

  }
    def processData(t: RDD[(Int, Int)], u: RDD[(Int, String)]): Map[Int, Long] = {
      var jn = t.leftOuterJoin(u).values.distinct
      val result1= jn.countByKey()
      return result1
    }
*/


}

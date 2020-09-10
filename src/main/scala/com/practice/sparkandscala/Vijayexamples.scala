package com.practice.sparkandscala
import scala.io.Source._
import org.apache.spark.SparkContext
import scala.io._
object Vijayexamples {


  def main(args:Array[String]): Unit =
  {

   /*

  // val data = List(1,2,3,4)
    //val b = data.map(x => x+2)
   // println(b)
  //  val c = b.map(x=> x *x)
   // println(c)
    //val d = c.take(3)
//val e = List(1,5,3,9,4,7)

//println(d)

val a= List(3,4,5,6)
val b =List(1,2,3,4)
//val c = a zip b
 //   println(c)
//val d = c.map((x,y)=>(x._1+y._1))

   //find the highest number

  val x = List(3,6,5,14,5)
    //val a = 0
    var e = x(0)
      for(i<-1 to (x.length-1)){
        if(x(i)> e )e = x(i)
              }
println(e)


 //def maxi(xs:List[Int])= {
 // val y= reduceLeft((x,x)=>x=>x)

//for(i<-a)
  //{
    //for(j<-b)
      //{
        //val y = (i +j)

    //println(y)
      //}


  //}
*/

//count manish how many times and uppercase
    val data = Seq("Abhishek","Manish","Abhi","Manish","Rahul","Manish")

    val cnt= data.flatMap(line=>line.split(",")).filter( x => x == "Manish" ).size
    val asd= data.map(x=>(x,1))
    println(cnt)
    println(asd)


   // val rd = s1.reduceByKey(_+_)
 // val asr = count(cnt).toString
    val upc = data.map(x=>x.toUpperCase)
    println(upc)



// to remove duplicates and filterd record with value >1
    val a = Map(("Abhi",1),("Man",2),("Rahul",3),("Abhi",1),("Man",2))

    val b = a.map(x=> x._1.distinct)
    println(b)
    val c = a.filter(x=>x._2 > 1)
    println(c)




 //to find count and unique

    val wd = Seq("abhi","abhi","abhi","man","abhi","man","rahul")
    val sd = wd.groupBy(x=>(x,1))

    println(sd)
    val zc = wd.map(x=>x!=1)
    println(zc)

  }


}

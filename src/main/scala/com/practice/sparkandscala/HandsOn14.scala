package com.practice.sparkandscala

object HandsOn14 extends App {
val input1:Int = args(0).toInt
  implicit val z = 5
  val v = (1 to input1).toList
//println(v)
def add(x:Int )(implicit y:Int=z) = x+y
  for(i<-v ) {
    val num = add(i)
    println(num)
  }
}

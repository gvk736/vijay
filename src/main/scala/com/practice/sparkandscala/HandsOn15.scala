package com.practice.sparkandscala

object HandsOn15 extends App {
  val input1 :Int = args(0).toInt
  val some:Option[Int] = Some(15)
  val none:Option[Int] = None
  val list = 1 to input1
  def findEven(x:Int):Boolean= {
    if(x % 2 == 0) {
      return true
    }
    else {
      return false
    }
  }
for(i<-list){
print(i)
if(findEven(i)){
  println("  is even")
} else {
  println("  is odd")
}
  }
}

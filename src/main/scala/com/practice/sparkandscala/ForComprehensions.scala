package com.practice.sparkandscala

object ForComprehensions extends App {

  val input : Int = args(0).toInt
 val x= for(i<- 1 to input) yield (i*5)
//println(x.toList)
val yieldList = for(i<-1 to input if(i<50 && i%2!=0) ) yield i*5

println(yieldList.toList)

  for(z<-yieldList if z%3==0)
println(z)
}

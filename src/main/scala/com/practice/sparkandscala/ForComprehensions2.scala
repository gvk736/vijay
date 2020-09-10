package com.practice.sparkandscala

object ForComprehensions2 extends App {

  val input : Int = args(0).toInt
  val list = 1 to input
  def add(x:Int,y:Int=5)= x + y
  for(number<- list) {
    val num = add(number)
    println("value :"+num)
  }
  case class Person(val name :String,val age : Int){}
  val personList :List[Person] = List(Person("Robert",56),Person("Chris",48),Person("Benedict",45),Person("Peter",47))
for(ele<- personList)
  println(ele.name+" :"+ele.age)

}

package com.practice.sparkandscala

object HandsOn5 extends App {

  val input = args(0)

  def convertToInt(s: String) =s.toInt
      convertToInt(input) match {

    case i:Int => println(+i,"Int")

    }

}
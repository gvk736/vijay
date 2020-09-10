package com.practice.sparkandscala

object Options extends App {
  val input = args(0)
  val capitals=Map("France"->"Paris","Japan"->"Tokyo","India"->"New Delhi","Russia"->"Moscow")
  val inputCapital=capitals.get(input)
  def show(a: Option[String])= a match {
    case Some(t) => t
    case None => "Invalid"
  }
  println(input+" : "+show(inputCapital))
}

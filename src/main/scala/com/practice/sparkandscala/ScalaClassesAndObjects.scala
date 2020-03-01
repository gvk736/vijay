package com.practice.sparkandscala

//first example
/* first
class Student{
  var id:Int = 0;                         // All fields must be initialized
  var name:String = null;
}


//second example
class Student(id:Int, name:String){     // Primary constructor
  def show(){
    println(id+" "+name)
  }
}


//third example

class Student(id:Int, name:String){
  def getRecord(){
    println(id+" "+name);
  }
}
*/
//Scala Anonymous object Example

class Arithmetic{
  def add(a:Int, b:Int){
    var add = a+b;
    println("sum = "+add);
  }
}

object ScalaClassesAndObjects {

  //first example
  /*
      def main(args:Array[String]){
      var s = new Student()               // Creating an object
      println(s.id+" "+s.name);
    }


//second example

  def main(args:Array[String]){
    var s = new Student(100,"Martin")   // Passing values to constructor
    s.show()                // Calling a function by using an object
  }

  //third example
  def main(args: Array[String]){
    var student1 = new Student(101,"Raju");
    var student2 = new Student(102,"Martin");
    student1.getRecord();
    student2.getRecord();
  }
*/
  //Scala Anonymous object Example
  def main(args:Array[String]){
    new Arithmetic().add(10,10);

  }


}
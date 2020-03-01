package com.practice.sparkandscala

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

object sparksqltextbookexamples1 extends App {

  val spark = SparkSession.builder().appName("spark sql examples textbook").master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  import spark.implicits._

  case class Customer(cId:Long,name:String,age:Int,gender:String)
  val customer=List(Customer(1,"James",21,"M"),Customer(2,"Liz",25,"F"),Customer(3,"John",31,"M"),Customer(4,"Jennifer",45,"F"),
    Customer(5,"Robert",41,"M"),Customer(6,"Sandra",45,"F"))
  val customerRDD = sc.parallelize(customer)
  //customerRDD.foreach(println)
  val customerDF=customerRDD.toDF()
 // customerDF.show()
  case class Product(pId: Long, name: String, price: Double, cost: Double)
  val products = List(Product(1, "iPhone", 600, 400),Product(2, "Galaxy", 500, 400),Product(3, "iPad", 400, 300),
    Product(4, "Kindle", 200, 100),Product(5, "MacBook", 1200, 900),Product(6, "Dell", 500, 400))

  val productRDD = sc.parallelize(products)
//productRDD.foreach(println)
  val productDF=productRDD.toDF()
  //productDF.show()

  case class Home(city: String, size: Int, lotSize: Int,bedrooms: Int, bathrooms: Int, price: Int)
  val homes = List(Home("San Francisco", 1500, 4000, 3, 2, 1500000),Home("Palo Alto", 1800, 3000, 4, 2, 1800000),                 Home("Mountain View", 2000, 4000, 4, 2, 1500000),
    Home("Sunnyvale", 2400, 5000, 4, 3, 1600000),  Home("San Jose", 3000, 6000, 4, 3, 1400000),
    Home("Fremont", 3000, 7000, 4, 3, 1500000),Home("Pleasanton", 3300, 8000, 4, 3, 1400000),
    Home("Berkeley", 1400, 3000, 3, 3, 1100000),Home("Oakland", 2200, 6000, 4, 3, 1100000),
    Home("Emeryville", 2500, 5000, 4, 3, 1200000))

  val homeRDD = sc.parallelize(homes)
 // homeRDD.foreach(println)
  val homeDF=homeRDD.toDF()
  //homeDF.show()


                        //Basic Operations
//1.cache: The cache method stores the source DataFrame in memory using a columnar format.
  // It scans only the required columns and stores them in compressed in-memory columnar format.
  // Spark SQL automatically selects a compression codec for each column based on data statistics.

  customerDF.cache()

//2.columns:The columns method returns the names of all the columns in the source DataFrame as an array of String.
  val cols= customerDF.columns
  //cols.foreach(println)

//3. dtypes:The dtypes method returns the data types of all the columns in the source DataFrame as an array of tuples.
  // The first element in a tuple is the name of a column and the second element is the data type of that column.
val Dtypes=customerDF.dtypes
//  Dtypes.foreach(println)

//4.explain:The explain method prints the physical plan on the console. It is useful for debugging.
  val expln = customerDF.explain()

//5. persist:The persist method caches the source DataFrame in memory.
  val persst = customerDF.persist

//6. printSchema:The printSchema method prints the schema of the source DataFrame on the console in a tree format.
  val prntscma= customerDF.printSchema()

//7. registertempTable:The registerTempTable method creates a temporary table in Hive metastore.
    // It takes a table name as an argument. A temporary table can be queried using the sql method in SQLContext or HiveContext.
   // It is available only during the lifespan of the application that creates it.
  val rgtmptbl = customerDF.registerTempTable("customer")
  val countDF = spark.sql("select count(1) as cnt from customer")
    //customerDF.show()
      //or
  customerDF.createOrReplaceTempView("customers1")

  val countDF123=spark.sql("select count(*) as cnt from customers1")
  //countDF123.show()


  //8. toDF:The toDF method allows you to rename the columns in the source DataFrame.
     // It takes the new names of the columns as arguments and returns a new DataFrame

  val resultDF = spark.sql("select count(1) from customer")
  val countDF1 = resultDF.toDF("cnt")
    //countDF1.show()

          //Language-Integrated Query Methods

//9. agg:The agg method performs specified aggregations on one or more columns in the source DataFrame and returns the result as a new DataFrame.
  val aggregates = productDF.agg(max("price"), min("price"), count("name"))
    //aggregates.show()

//10. apply:The apply method takes the name of a column as an argument and returns the specified column in the source DataFrame as an instance of the Column class.
    // The Column class provides operators for manipulating a column in a DataFrame.

  //val priceColumn = productDF.apply("price")
  //val discountedPriceColumn = priceColumn * 0.5
  val priceColumn = productDF("price")
  val discountedPriceColumn = priceColumn * 0.5

  val aggregates1 = productDF.agg(max("price"), min("price"), count("name"))
//or
val aggregates2 = productDF.agg(max(productDF("price")), min(productDF("price")),count(productDF("name")))

 //The expression productDF("price") can also be written as $"price" for convenience. Thus, the following two expressions are equivalent.
 val aggregates3 = productDF.agg(max($"price"), min($"price"), count($"name"))
  val aggregates2e = productDF.agg(max(productDF("price")), min(productDF("price")),count(productDF("name")))

  //In summary, the following three statements are equivalent.
  val aggregates2ex = productDF.agg(max(productDF("price")), min(productDF("price")),count(productDF("name")))
  val aggregates1e = productDF.agg(max("price"), min("price"), count("name"))
  val aggregates3e = productDF.agg(max($"price"), min($"price"), count($"name"))

//11. cube: It takes a list of columns and applies aggregate expressions to all possible combination of the grouping columns.
      // The cube method takes the names of one or more columns as arguments and returns a cube for  multi-dimensional analysis. It is useful for generating cross-tabular reports.
     // Assume you have a dataset that tracks sales along three dimensions: time, product and country.
     // The cube method allows you to generate aggregates for all the possible combinations of the dimensions that you are interested in.

  case class SalesSummary(date: String, product: String, country: String, revenue: Double)
  val sales = List(SalesSummary("01/01/2015", "iPhone", "USA", 40000),SalesSummary("01/02/2015", "iPhone", "USA", 30000),
    SalesSummary("01/01/2015", "iPhone", "China", 10000),SalesSummary("01/02/2015", "iPhone", "China", 5000),
    SalesSummary("01/01/2015", "S6", "USA", 20000),SalesSummary("01/02/2015", "S6", "USA", 10000),
    SalesSummary("01/01/2015", "S6", "China", 9000),SalesSummary("01/02/2015", "S6", "China", 6000))

  val salesDF = sc.parallelize(sales).toDF()
       //salesDF.foreach(println)
       //salesDF.toDF().show()
  val salesCubeDF = salesDF.cube($"date", $"product", $"country").sum("revenue")
 val x= salesCubeDF.withColumnRenamed("sum(revenue)", "total")
      //  x.show(30)
  val y =salesCubeDF.filter("product IS null AND date IS null AND country='USA'")
      //  y.show
  val z= salesCubeDF.filter("date IS null AND product IS NOT null AND country='USA'")
     //z.show

//12. distinct:The distinct method returns a new DataFrame containing only the unique rows in the source DataFrame.
  val dfWithoutDuplicates = customerDF.distinct
       //dfWithoutDuplicates.show()

//13. explode:The explode method generates zero or more rows from a column using a user-provided function.
      // It takes three arguments. The first argument is the input column, the second argument is the output column and
      // the third argument is a user provided function that generates one or more values for the output column for each value in the input column.
  case class Email(sender: String, recepient: String, subject: String, body: String)
  val emails = List(Email("James", "Mary", "back", "just got back from vacation"),
    Email("John", "Jessica", "money", "make million dollars"),Email("Tim", "Kevin", "report", "send me sales report ASAP"))

  val emailDF = sc.parallelize(emails).toDF()
  val wordDF = emailDF.explode("body", "word") { body: String => body.split(" ")}
      // wordDF.show

//14. filter:The filter method filters rows in the source DataFrame using a SQL expression provided to it as an argument.
     // It returns a new DataFrame containing only the filtered rows. The SQL expression can be passed as a string argument.

  val filteredDF = customerDF.filter("age > 25")
     //filteredDF.show()
    // A variant of the filter method allows a filter condition to be specified using the Column type.
  val filteredDF1 = customerDF.filter($"age" > 25)
       //As mentioned earlier, the preceding code is a short-hand for the following code.
  val filteredDF2 = customerDF.filter(customerDF("age") > 25)

//15. groupBy:The groupBy method groups the rows in the source DataFrame using the columns provided to it as arguments.
        // Aggregation can be performed on the grouped data returned by this method.
  val countByGender = customerDF.groupBy("gender").count
      //countByGender.show()
  val revenueByProductDF = salesDF.groupBy("product").sum("revenue")
     //revenueByProductDF.show()

//16. intersect:The intersect method takes a DataFrame as an argument and returns a new DataFrame containing only the rows in both the input and source DataFrame.
val customers2 = List(Customer(11, "Jackson", 21, "M"),Customer(12, "Emma", 25, "F"),Customer(13, "Olivia", 31, "F"),
  Customer(4, "Jennifer", 45, "F"),Customer(5, "Robert", 41, "M"),Customer(6, "Sandra", 45, "F"))
  val customer2DF = sc.parallelize(customers2).toDF()
  val commonCustomersDF = customerDF.intersect(customer2DF)
      //commonCustomersDF.show

//17. join:The join method performs a SQL join of the source DataFrame with another DataFrame. It takes three arguments, a DataFrame, a join expression and a join type.
case class Transaction(tId: Long, custId: Long, prodId: Long, date: String, city: String)
  val transactions = List(Transaction(1, 5, 3, "01/01/2015", "San Francisco"),Transaction(2, 6, 1, "01/02/2015", "San Jose"),
    Transaction(3, 1, 6, "01/01/2015", "Boston"),Transaction(4, 200, 400, "01/02/2015", "Palo Alto"),
    Transaction(6, 100, 100, "01/02/2015", "Mountain View"))
  val transactionDF = sc.parallelize(transactions).toDF()
  val innerDF = transactionDF.join(customerDF, $"custId" === $"cId", "inner")
       //innerDF.show()
       val outerDF = transactionDF.join(customerDF, $"custId" === $"cId", "outer")
       //outerDF.show
       val leftOuterDF = transactionDF.join(customerDF, $"custId" === $"cId", "left_outer")
        //leftOuterDF.show
        val rightOuterDF = transactionDF.join(customerDF, $"custId" === $"cId", "right_outer")
         //rightOuterDF.show

//18. limit:The limit method returns a DataFrame containing the specified number of rows from the source DataFrame.

  val fiveCustomerDF = customerDF.limit(5)
        //fiveCustomerDF.show

//19. orderBy:The orderBy method returns a DataFrame sorted by the given columns. It takes the names of one or more columns as arguments.
val sortedDF = customerDF.orderBy("name")
        //sortedDF.show
  val sortedByAgeNameDF = customerDF.sort($"age".desc, $"name".asc)
         //sortedByAgeNameDF.show

//20. randomsplit:The randomSplit method splits the source DataFrame into multiple DataFrames.
        // It takes an array of weights as argument and returns an array of DataFrames.
        // It is a useful method for machine learning, where you want to split the raw dataset into training, validation and test datasets.
val dfArray = homeDF.randomSplit(Array(0.6, 0.2, 0.2))
  dfArray(0).count
  dfArray(1).count
  dfArray(2).count
//21. rollup:Which computes hierarchical subtotals from left to right
     // The rollup method takes the names of one or more columns as arguments and returns a multi-dimensional rollup.
      // It is useful for subaggregation along a hierarchical dimension such as geography or time.
     // Assume you have a dataset that tracks annual sales by city, state and country. The rollup method can be used to calculate both grand total and subtotals by city, state, and country.
  case class SalesByCity(year: Int, city: String, state: String,country: String, revenue: Double)
  val salesByCity = List(SalesByCity(2014, "Boston", "MA", "USA", 2000),SalesByCity(2015, "Boston", "MA", "USA", 3000),
    SalesByCity(2014, "Cambridge", "MA", "USA", 2000),SalesByCity(2015, "Cambridge", "MA", "USA", 3000),
    SalesByCity(2014, "Palo Alto", "CA", "USA", 4000),SalesByCity(2015, "Palo Alto", "CA", "USA", 6000),
    SalesByCity(2014, "Pune", "MH", "India", 1000),SalesByCity(2015, "Pune", "MH", "India", 1000),
    SalesByCity(2015, "Mumbai", "MH", "India", 1000),SalesByCity(2014, "Mumbai", "MH", "India", 2000))
  val salesByCityDF = sc.parallelize(salesByCity).toDF()
  val rollup = salesByCityDF.rollup($"country", $"state", $"city").sum("revenue")
    //rollup.show

//22. sample:The sample method returns a DataFrame containing the specified fraction of the rows in the source DataFrame.
  // It takes two arguments. The first argument is a Boolean value indicating whether sampling should be done with replacement.
  // The second argument specifies the fraction of the rows that should be returned.
    val sampleDF = homeDF.sample(true, 0.10)
     //sampleDF.show()

//23. select:The select method returns a DataFrame containing only the specified columns from the source DataFrame.
  val namesAgeDF = customerDF.select("name", "age")
     //namesAgeDF.show
     val newAgeDF = customerDF.select($"name", $"age" + 10)
         //newAgeDF.show
//24. selectExpr:The selectExpr method accepts one or more SQL expressions as arguments and returns a DataFrame generated by executing the specified SQL expressions.
  val newCustomerDF = customerDF.selectExpr("name", "age + 10  AS new_age","IF(gender = 'M', true, false) AS male")
      //newCustomerDF.show()

//25. withColumn: The withColumn method adds a new column to or replaces an existing column in the source DataFrame and returns a new DataFrame.
    // It takes two arguments. The first argument is the name of the new column and the second argument is an expression for generating the values of the new column.
  val newProductDF = productDF.withColumn("profit", $"price" - $"cost")
     //newProductDF.show


                     //RDD Operations
import org.apache.spark.sql.Row
//1.rdd
 val rdd = customerDF.rdd
val firstRow= rdd.first
//  println(firstRow)
  val name = firstRow.getString(1)
//println(name)
val age = firstRow.getInt(2)
  //println(age)
val nameAndAge = rdd.map{case Row(cId:Long,name:String,age:Int,gender:String)=>(name,age)}
// nameAndAge.collect().foreach(println)

//2. toJSON

  val jsonRDD = customerDF.toJSON
  //jsonRDD.collect().foreach(println)

       //Actions
//1. collect
  val result=customerDF.collect()
    //result.foreach(println)
//2.count
  val count1 = customerDF.count()
  //println(count1)
//3.describe
  val summaryStatsDF = productDF.describe("price","cost")
  //summaryStatsDF.show()
//4. first
  val first= customerDF.first()
  //println(first)
//5.show
  customerDF.show(2)
//6. take
  val first2Rows=customerDF.take(2)
 // first2Rows.foreach(println)

//customerDF.write.format("org.apache.spark.sql.csv").csv("D:\\vijay\\aict\\ai")

  val client = Seq((1,"A",10),(2,"A",5),(3,"B",56)).toDF("ID","Categ","Amnt")
  client.show()

}
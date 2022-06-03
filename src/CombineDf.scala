import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object combineDF extends App {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","combineDF")
  sparkConf.set("spark.master","local[2]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
 
val myList = List(
      ("kavi","eng",98,2009),
      ("kavi","math",100,2009),
      ("mani","eng",99,2001),
      ("mani","math",100,2001))
      
      
val myList1 = List(
      ("eng","udhaya",98,2009),
      ("math","udhaya",100,2009),
      ("eng","prathu",99,2020),
      ("eng","prathu",100,2020))

         
 val empDf = spark.createDataFrame(myList).toDF("name","sub","mark","year")
 
 val empDf1 = spark.createDataFrame(myList1).toDF("sub","name","mark","year")
         
                       
 val combineDf=empDf.unionByName(empDf1)
 
val result = combineDf.groupBy("sub")
               .pivot("name")
               .sum("mark")
               .na.fill(0)
               
result.show()
}
         

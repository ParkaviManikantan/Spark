import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object groupBydep extends App {
  
  val sparkConf = new SparkConf
  sparkConf.set("spark.app.name","my app")
  sparkConf.set("spark.master","local[*]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val myList = List(
      ("sales",10000),
      ("sales",20000),
      ("marketing",10000),
      ("marketing",20000))
      
  val empDf = spark.createDataFrame(myList).toDF("dept","sal")
  
  val grpDf = empDf.select("dept")
              .groupBy("dept")
              .count()
              
  grpDf.show()
  
  
}
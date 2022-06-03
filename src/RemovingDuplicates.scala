import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object RemovingDuplicates extends App {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","combineDF")
  sparkConf.set("spark.master","local[2]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val dataList = List((101, "Mumbai", "Goa"),
                 (102, "Mumbai", "Bangalore"),
                 (102, "Mumbai", "Bangalore"),
                 (103, "Delhi", "Chennai"),
                 (104, "Bangalore", "Kolkata"))
                 
                 
  val df = spark.createDataFrame(dataList).toDF("id","source","Destination")
  
  val dup = df.count-df.distinct().count()
  
  if (dup>0) println("Duplicate row exist")

  
  
}
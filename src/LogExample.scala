import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class Logging(level:String, datetime: String)

object LogExample extends App {
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","combineDF")
  sparkConf.set("spark.master","local[2]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
 
  val myList=List("WARN,2016-12-13 04:19:32",
                   "FATAL,2016-12-13 03:22:34",
                   "WARN,2016-12-13 03:21:21",
                   "INFO,2015-04-21 14:32:21",
                   "FATAL,2015-04-21 19:23:20")
  import spark.implicits._
                   
  val rdd = spark.sparkContext.parallelize(myList)
  
  def mapper(line: String):Logging = {
    val fields = line.split(",")
    val logging:Logging = Logging(fields(0),fields(1))
    return logging
  }
  
  val rdd2 = rdd.map(mapper)
  val df = rdd2.toDF().show()
  
}
  
  
  


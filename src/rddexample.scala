import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object rddexample extends App {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my app")
  sparkConf.set("spark.master","local[2]")
 
  val spark = SparkSession.builder
  .config(sparkConf)
  .getOrCreate()
 
  val follow = List((1,"k"),(9,"o"),(4,"r"),(5,"y"),(6,"g"),(2,"e"))
  
  val df = spark.createDataFrame(follow).toDF("id","name")
  
  var notFollowingList=List(9,8,7,6,3,1)
  
  df.filter(!df.col("id").isin(notFollowingList:_*)).show()
}






/*val list = List("a1234","b1234","a2345")
 
  for (result <- list) 
    if (result.startsWith("a")) println(result.substring(1,result.length))
    else println(result) */

 /*val sc = new SparkContext("local[*]","myapp")
  val text = sc.parallelize(list) 
  val results = text.collect() */
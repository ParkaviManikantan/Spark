import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.Row


object LoggingLevel extends App {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my app")
  sparkConf.set("spark.master","local[*]")
 
  val spark = SparkSession.builder
  .config(sparkConf)
  .getOrCreate()
  
  val logSchema = StructType(List(
      StructField("Level", StringType),
      StructField("DateTime",TimestampType)
      ))
  
  val logDf = spark.read
      .format("csv")
      .option("header",true)
      .schema(logSchema)
      .option("path","C:/Users/kavimani/Desktop/Input/biglog.txt")
      .load()
 
 val wind = Window
   .partitionBy("Level","DateTime")
   .orderBy("DateTime")
 
 val finDf = logDf.withColumn("Count", count("Level").over(wind))
                  .withColumn("Month",from_unixtime(unix_timestamp(col("DateTime"),"yyyy-MM-dd hh:mm:ss"),"M"))
                  
 
 val columns = List("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug",
                    "Sep","Oct","Nov","Dec")
                    

 val finDf1 = finDf.select("Level","Month")
      .groupBy("Level")
      .pivot("Month", columns)
      


 
 
             
}
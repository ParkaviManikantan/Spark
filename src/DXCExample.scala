import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DXCExample extends App
{
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my app")
  sparkConf.set("spark.master","local[2]")
 
  val spark = SparkSession.builder
  .config(sparkConf)
  .getOrCreate()
  
  val empData = List(
      (12,"A","D","2021-06-01"),
      (13,"B","C","2021-06-01"),
      (14,"C","D","2021-06-01"),
      (13,"B","C","2021-06-02"),
      (14,"C","D","2021-06-02"),
      (12,"A","C","2021-06-02"),
      (14,"C","D","2021-06-03"))
      
 val empDf = spark.createDataFrame(empData)
             .toDF("Empid","Empname","Transaction","Timestamp")
          
 val windSpec = Window
 .partitionBy(col("Empid"),col("EmpName"),col("Transaction"))
 .orderBy(col("Timestamp") desc)
 
val finalDf = empDf.withColumn("ranks",row_number().over(windSpec))
              .select("Empid","EmpName","Transaction","Timestamp")
              .orderBy(col("Empid"),col("Timestamp"))
               .filter("ranks=1")
                            
finalDf.show()               

}
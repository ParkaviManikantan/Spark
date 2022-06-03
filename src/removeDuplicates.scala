
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object removeDuplicates extends App{

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my app")
  sparkConf.set("spark.master","local[2]")
 
  val spark = SparkSession.builder
  .config(sparkConf)
  .getOrCreate()
 
  val input= List (("Udhaya","udhayanvitesu@gmail.com"),
                    ("Ritika","udhayanvitesu@gmail.com"),
                    ("Klive","klivegopala@gmail.com"),
                    ("Arjun","udhayadharini.gk@gmail.com"),
                    ("Dharini","udhayadharini.gk@gmail.com")
                    )
                    
   val inputDF=spark.createDataFrame(input).toDF("Name","Email")
 //  inputDF.printSchema()
   
   //inputDF.dropDuplicates("Email").show(false)
   
  inputDF.createOrReplaceTempView("PersonalDetail")
  //spark.sql("select Email, count(Email) as count from PersonalDetail groupBy Email having count >1").show()
   
  spark.sql("select Email, count(1) as Count from PersonalDetail group by Email having count > 1").show() 
}
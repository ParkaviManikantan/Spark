import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions._

object PromoOfferViolation extends App
{

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my app")
  sparkConf.set("spark.master","local[2]")
 
  val spark = SparkSession.builder
  .config(sparkConf)
  .getOrCreate()
  
  val ownerInp = List(
      ("mob",100),
      ("lap",1000))
      
  val ownerDf = spark.createDataFrame(ownerInp).toDF("ProdId","Amt")    
  
  ownerDf.createOrReplaceTempView("Owner")
  
  val promoInp = List (
      ("XXXXX","mob",90,"2021-04-02","2021-04-03"),
      ("YYYYY","lap",900,"2021-04-02","2021-04-05"))
      
  val promoDf = spark.createDataFrame(promoInp).toDF("PatnerId","ProdId","Price","StartDate","EndDate")
  
  val promo1Df = promoDf.withColumn("StartDate",from_unixtime(unix_timestamp(col("StartDate"),"yyyy-MM-dd")))
                  .withColumn("EndDate",from_unixtime(unix_timestamp(col("EndDate"),"yyyy-MM-dd")))
      
  promo1Df.createOrReplaceTempView("Promo")
  
  val sellInp = List(
      ("XXXXX","mob","90","2021-04-02"),
      ("XXXXX","mob","90","2021-04-04"),
      ("XXXXX","mob","100","2021-04-05"),
      ("YYYYY","lap","900","2021-04-02"),
      ("YYYYY","lap","900","2021-04-06"),
      ("YYYYY","lap","1100","2021-04-10"))
      
   val sellerDf = spark.createDataFrame(sellInp).toDF("PatnerId","ProdId","ProdAmt","Date")
   
    val seller1Df = sellerDf.withColumn("Date",from_unixtime(unix_timestamp(col("Date"),"yyyy-MM-dd")))
    
    seller1Df.createOrReplaceTempView("Seller")
   
   spark.sql("""select p.PatnerId, p.ProdId, t.ProdAmt, t.Date from Promo p join (select s.PatnerId, s.ProdId, 
          s.ProdAmt, s.Date from Seller s join Owner o on s.ProdId = o.ProdId 
          and s.ProdAmt < o.Amt) t  on p.PatnerId = t.PatnerId and p.ProdId = t.ProdId 
          where t.Date not between p.StartDate and p.EndDate""").show() 
}
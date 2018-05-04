import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
case class Analytics(Samsung:Long,Apple:Long,LGE:Long,Motorola:Long,Google:Long,Xiaomi:Long,
                     LG:Long,OnePlus:Long,Huawei:Long,Sony:Long)

case class PlatformAnalytics(iOS:Long,Android:Long)

object Netgear_Devices {

  val spark = SparkSession.builder()
    .appName("Netgear Devices Analysis")
    .master("yarn")
    .enableHiveSupport()
    .getOrCreate()

  val df = spark.read.format("csv")
    .option("header",true)
    .option("inferSchema", "true")
    .load("hdfs:///tmp/Netgear-Appsee/analytics-devices.csv")

  def main(args: Array[String]): Unit = {
    df.printSchema()


    // df.withColumn("Manufacturer",ApplicationUtils.toCapatilize(df("Manufacturer")))
    //val newdf = df.withColumn("Manufacturer", upper(df("Manufacturer"))).cache()
    // df.withColumn("Manufacturer", when(df("Manufacturer").equalTo("samsung"),"Samsung").otherwise(col("Manufacturer")))
    val new_df = df.withColumn("Manufacturer",ApplicationUtils.toCapatilize(df("Manufacturer"))).cache()

    new_df.show(35)

    val samsungDevices = new_df.filter(new_df("Manufacturer") === "Samsung").count()
    val appleDevices = new_df.filter(new_df("Manufacturer") === "Apple").count()
    val lgeDevices = new_df.filter(new_df("Manufacturer") === "LGE").count()
    val motorolaDevices = new_df.filter(new_df("Manufacturer") === "Motorola").count()
    val googleDevices = new_df.filter(new_df("Manufacturer") === "Google").count()
    val xiomiDevices = new_df.filter(new_df("Manufacturer") === "Xiaomi").count()
    val LGDevices = new_df.filter(new_df("Manufacturer") === "LG").count()
    val onePlusDevices = new_df.filter(new_df("Manufacturer") === "OnePlus").count()
    val huaweiDevices = new_df.filter(new_df("Manufacturer") === "HUAWEI").count()
    val sonyDevices = new_df.filter(new_df("Manufacturer") === "Sony").count()


    val iosDevices = new_df.filter(new_df("Platform") === "iOS").count()
    val androidDevices = new_df.filter(new_df("Platform") === "Android").count()

   /* val stats = spark.sparkContext.parallelize(Seq(samsungDevices,lgeDevices,motorolaDevices,googleDevices,xiomiDevices,LGDevices
    ,onePlusDevices,huaweiDevices,sonyDevices,iosDevices,androidDevices)).toDF*/

    val rdd1 = spark.sparkContext.parallelize(Seq(Analytics(samsungDevices,appleDevices,lgeDevices,motorolaDevices,googleDevices
    ,xiomiDevices,lgeDevices,onePlusDevices,huaweiDevices,sonyDevices)))

    val rdd2 = spark.sparkContext.parallelize(Seq(PlatformAnalytics(iosDevices,androidDevices)))

    val manufacturerAnalytics = spark.createDataFrame(rdd1)
    val platformAnalytics = spark.createDataFrame(rdd2)

    manufacturerAnalytics.show()
    platformAnalytics.show()

    new_df.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("Netgear.devices")
    manufacturerAnalytics.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("Netgear.manufacturers")
    platformAnalytics.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("Netgear.platform")

  }
}

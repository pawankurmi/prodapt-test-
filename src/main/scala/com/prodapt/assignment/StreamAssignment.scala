package com.prodapt.assignment
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object StreamAssignment {

  def main(args: Array[String]): Unit ={
    val appName = "prodapt-assignment"
    val srcdir = args(0)
    val destdir = args(1)
    val checkptdir = destdir+"/"+"checkpt/"

    val spark = SparkSession.builder.appName(appName).getOrCreate()
    import spark.implicits._
    //schema for source file
    val schema = StructType(
      List(
        StructField("message", StringType, true)))
    //Read message files from src path
    var df = spark.readStream.schema(schema).json(srcdir)
// filtering and parshing the data
       df = df.filter(col("message").contains("omwssu")).withColumn("timestamp", concat(split(col("message"), " ").getItem(9),lit(" "), split(col("message"), " ").getItem(17))).withColumn("timestamp", date_format($"timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    //  extracting required field
    df = df.withColumn("message", split(col("message"), "GET").getItem(1)).withColumn("message", split(trim(col("message")), "1.1").getItem(0))
    df = df.withColumn("fqdn", concat(split(trim(col("message")), "/").getItem(0), lit("//"), split(trim(col("message")), "/").getItem(2), lit("/"), split(trim(col("message")), "/").getItem(3)))
    df = df.withColumn("cpe_id", split(trim(col("message")), "/").getItem(4))
    df = df.withColumn("action", split(trim(col("message")), "/").getItem(5))
    df = df.withColumn("error_code", concat(split(trim(col("message")), "/").getItem(6), lit("."), split(trim(col("message")), "/").getItem(7)))
    df = df.withColumn("error_code", df("error_code").cast(DoubleType))
    df = df.select("fqdn","cpe_id", "action", "error_code" , "message", "timestamp")


    df.writeStream.format("json").option("path", destdir).option("checkpointLocation",checkptdir).outputMode("append").start()

  }

}

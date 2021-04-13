package com.prodapt.assignment
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions._

object SreamAssignment {

  def main(args: Array[String]): Unit ={
    val appName = "prodapt-assignment"
    val srcfile = args(0)
    val destdir = args(1)

    val spark = SparkSession.builder.appName(appName).getOrCreate()

      //Read message files from src path
      val rawrdd = spark.readStream.textFile(srcfile).filter(msg => msg.contains("omwssu"))
      val msgrdd = rawrdd.map{row => row.split("\\t")}.map{cols => MassageSchema(cols(13).substring(0,46), cols(13).substring(46,70), cols(13).substring(70,79), cols(13).substring(81,85).replaceAll("\\/","\\."), cols(13) , (cols(2) + cols(3) + cols(4))) }
      val msgdf = msgrdd.toDF()
     val ts = to_timestamp(col("dts"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      msgdf.withColumn("ts", ts)

      msgdf.write.option("header" , true).option("compression", "snapy").csv(destdir)
  }

}

case class MassageSchema (fdqn :String , cpe_id :String , action : String, error_code : String ,message :String , timestamp :String )

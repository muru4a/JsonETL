package com.etl.sparkenv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkEnv {

 val sparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "1024m")
    .set("spark.kryoserializer.buffer", "512m")
  //  .set("spark.akka.frameSize", "1000")
    .set("spark.driver.maxResultSize", "4g")
    .set("spark.hadoop.mapred.output.compress", "true")
    .set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    .set("spark.hadoop.mapred.output.compression.type", "BLOCK")
    .set("es.http.timeout", "30s")
    .set("es.scroll.size", "50")
    .set("es.index.auto.create", "true")
    // .setAppName("Sample_Application")
    .setMaster("local[2]")

   //val sc = new SparkContext(sparkConf)

  //val sc= new SparkContext(sparkConf)

  val spark = SparkSession
    .builder()
    .appName("Solar Prediction")
    .config(sparkConf)
    .getOrCreate()


 /* val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Spark JSON Rules Engine")
    .getOrCreate() */
}

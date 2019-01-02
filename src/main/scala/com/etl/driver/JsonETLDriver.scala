package com.etl.driver

import com.etl.sparkenv.SparkEnv
import com.etl.methods.JsonTransform._
import org.apache.log4j.Level


object JsonETLDriver {

  val log = org.apache.log4j.LogManager.getLogger("json_log")

  log.setLevel(Level.INFO)

  def main(args: Array[String]) {

     log.info("Starting Rules Engine")

 // if (args.length < 3) throw new IllegalArgumentException("Usage: com.etl.driver [JsonFile] [InputFile] [OutputFile]")

    //Input parameters
   // val JsonFile = args(0)
    val JsonFile= "/Users/murugesh/Documents/transform-spec.json"
   // val InputFile = args(1)
   val InputFile ="/Users/murugesh/Documents/SolarPrediction.csv"
   // val OutputFile= args(2)
   val OutputFile="/Users/murugesh/Documents/output/output.csv"

    import com.etl.sparkenv.SparkEnv.spark._

    //Input CSV Reader

    val InputCSVDf = SparkEnv.spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(InputFile)

    //Transformation Json Reader

    val JsonDF = SparkEnv.spark.read.option("multiline", true).json(JsonFile)

    //Processing ETL

    val TransformDF = InputCSVDf.transform(transformations)

    //Output CSV Writer

    TransformDF.write.format("com.databricks.spark.csv").save(OutputFile)


  }

}

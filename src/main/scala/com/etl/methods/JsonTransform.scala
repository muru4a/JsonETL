package com.etl.methods

import com.etl.sparkenv.SparkEnv._
import com.etl.methods.JsonRules.{FtoC, hst_to_unix, lowerRemoveAllWhitespace}
import com.etl.utils.JsonUtils
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, unix_timestamp}

import scala.collection.immutable.Map

object JsonTransform  {

  def transform(df:DataFrame): DataFrame= {

  var simpleColumns: List[Column] = List.empty[Column]
  var complexColumns: List[Column] = List.empty[Column]


  val flattened_df = JsonUtils.flattenDataFrame(df)


  val rulesDF = flattened_df.withColumn("rules", concat_ws("", col("transforms_operation"), col("transforms_column")))

  rulesDF.show(false)

  print(rulesDF.schema)

  val testMap: scala.collection.immutable.Map[String, Any] = Map()
  //val df= spark.sql("select address,exists from testTable")
  flattened_df.collect.map(r => {
    val key = r(1).toString
    val value = r(2)
    testMap + (key -> value)
    // testMap+ =(key -> value)
  }
  )

  testMap.foreach(println(_))

    flattened_df

    }


  def transformations(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (InputDf, colName) =>
      InputDf.withColumn("RecordLocation", lowerRemoveAllWhitespace(col("RecordLocation")))
        .withColumn("Temperature", FtoC(col("Temperature")))
        .withColumn("HST_COL1",unix_timestamp(hst_to_unix(concat_ws(" ",col("RecordedDate"),col("RecordedTime")))))
        .withColumn("HST_COL2",unix_timestamp(hst_to_unix(concat_ws(" ",col("RecordedDate"),col("TimeSunRise")))))
        .withColumn("HST_COL3",unix_timestamp(hst_to_unix(concat_ws(" ",col("RecordedDate"),col("TimeSunSet")))))
    }
  }


}


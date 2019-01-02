package com.etl.methods

import java.text.SimpleDateFormat
import java.util.{SimpleTimeZone, TimeZone}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

import scala.collection.immutable.Map

/**
  * Json Rules to build the logic for processing
  *
  * @author Murugesan Alagusundaram
  *
  */

object JsonRules {

  def lowerRemoveAllWhitespace(col: Column): Column = {

    // col.toString().toLowerCase().replaceAll("""[\p{Punct}&&[^.]]""", " ").replaceAll("\\s", "-")

    lower(regexp_replace(col, "\\s+", "-"))

  }


  def FtoC(col: Column): Column = {

    (col - 32) / 1.8

  }


  def concatcol(con: String, con1: String) = {

    con.concat(con1)

  }

  val hst_to_unix = udf((dateInString: String) => {

    val formatter = new SimpleDateFormat("MM/dd/yy hh:mm:ss")
    formatter.setTimeZone(TimeZone.getTimeZone("Pacific/Honolulu"))

    //val dateInString = "9/29/16 23:55:26"

    val date = formatter.parse(dateInString)
    formatter.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
    val dateAsString = formatter.format(date)

    //val formattedDateString = formatter.format(date)
    //val finalFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    //val finalDate = finalFormatter.parse(dateAsString)

    val inputFormat = new SimpleDateFormat("MM/dd/yy HH:mm:ss")
    val outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val formattedDate = outputFormat.format(inputFormat.parse(dateAsString))
    //println(formattedDate)

    formattedDate
    //  date_format(unix_timestamp(col,"MM/dd/yy hh:mm:ss").cast("Timestamp").alias("txn_date_millis"),"yyyy-MM-dd hh:mm:ss")

    //val timeUdf = udf{(time: java.sql.Timestamp) => new java.sql.Timestamp(time.getTime + 24*60*60*1000 - 1000)}


    //  unix_timestamp(to_utc_timestamp(col,"Pacific/Honolulu"))

  })

}

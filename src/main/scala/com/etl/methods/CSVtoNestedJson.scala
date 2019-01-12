package com.etl.methods

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, types}
import org.apache.log4j.Level


object CSVtoNestedJson {

   val log = org.apache.log4j.LogManager.getLogger("json_log")

   log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

   log.info("Starting Rules Engine")

    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max","1024m")
      .set("spark.kryoserializer.buffer", "512m")
      .set("spark.akka.frameSize", "1000")
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.hadoop.mapred.output.compress", "true")
      .set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
      .set("spark.hadoop.mapred.output.compression.type", "BLOCK")
      .set("es.http.timeout", "30s")
      .set("es.scroll.size", "50")
      .set("es.index.auto.create", "true")
      .setAppName("Sample_Application")
      .setMaster("local")

   val sc = new SparkContext(sparkConf)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .getOrCreate()

   if (args.length < 3) throw new IllegalArgumentException("Usage: com.etl.CSVtoNestedJson [events] [demo] [OutputFile]")

   //Input parameters
   val EventsFile = args(0)
   val DemoFile = args(1)
   val OutputFile= args(2)


    val eventsDf = spark.read.
      .format("com.databricks.spark.csv")
      .option("delimiter", "|")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/murugesh/Documents/events.psv")

    eventsDf.printSchema()
    eventsDf.show(10)

    val demoDf = spark.read.
      .format("com.databricks.spark.csv")
      .option("delimiter", "|")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/murugesh/Documents/demo.psv")


    demoDf.printSchema()
    demoDf.show(10)


    eventsDf.createOrReplaceTempView("events")
    demoDf.createOrReplaceTempView("patients")

   //Drive the Json format

    val joinDF=spark.sqlContext.sql("select birth_date,gender,struct(date," +
      " case when icd_version='9' then 'http://hl7.org/fhir/sid/icd-9-cm'" +
      "     when icd_version='10' then 'http://hl7.org/fhir/sid/icd-10'" +
      "     else 'NA'" +
      "end as system" +
      ",icd_code as code ) as events " +
      " from events e inner join patients p on e.patient_id=p.patient_id" +
      " where  birth_date is not null  and gender is not null" )

    joinDF.createOrReplaceTempView("temptable")

    val finalJsonDF=spark.sqlContext.sql("select birth_date,gender,collect_list(events) as events from temptable" +
      " group by birth_date,gender")


   finalJsonDF.toJSON.take(2).foreach(println(_))

   //Write the Json format in text file

   finalJsonDF.toJSON.coalesce(2).
     write.mode("overwrite").
     format("text").
     save(OutputFile)


    // Total number of valid patients

   val valpatients=spark.sqlContext.sql("select count(distinct p.patient_id) from patients p inner join events e on p.patient_id=e.patient_id"  +
     "where p.birth_date !='' and p.gender !='' ")


   print("Total number of valid patients :" )

   print( valpatients.show())


   //Maximum/Minimun/Median length of patient timelines in days

   val MaxDF= spark.sqlContext.sql("select patient_id,max(date) as max_date  from events group by patient_id")

   val MinDF=spark.sqlContext.sql("select patient_id,min(date) as min_date from events group by patient_id")

   MaxDF.createOrReplaceTempView("max_temptable")
   MinDF.createOrReplaceTempView("min_temptable")

   val diffDf=spark.sqlContext.sql("select m1.patient_id,datediff(to_date(max_date),to_date(min_date)) as length from max_temptable m1 inner join min_temptable m2 on m1.patient_id=m2.patient_id ")

   diffDf.createOrReplaceTempView("length_temptable")

   //Calculate the Rank of Patients based on the Length of patient timelines in days
   val lengthDf=spark.sqlContext.sql("select patient_id,length,rank() over(order by length) min_rnk,rank() over(order by length desc) max_rnk from length_temptable ")

   lengthDf.createOrReplaceTempView("finaltable")

   //Maximum length of patient timelines in days
   val maxlengthDf= spark.sqlContext.sql("select * from finaltable where max_rnk=1 ")

   print("Maximum length of patient timelines in days:" + maxlengthDf.show())

   //Minimum length of patient timelines in days
   val minlengthDf= spark.sqlContext.sql("select * from finaltable where min_rnk=1")

   print("Minimum length of patient timelines in days" + minlengthDf.count())

   //Median length of patient timelines in days
   val median=spark.sqlContext.sql("select percentile_approx(length,0.5) FROM finaltable")

   print("Median length of patient timelines in days:" + median.show())

   //Count of males and females

   val cnt=spark.sqlContext.sql("select gender,count(*) from patients group by gender order by gender asc")

   print("Count of males and females: " + cnt.show())

   //Maximum/Minimum/Median age of patient as calculated between birthdate and
   //last event in timeline


   val maxageDF=spark.sqlContext.sql("select p.patient_id,datediff(to_date(max_date),to_date(birth_date)) m_date from patients p inner join max_temptable e on p.patient_id=e.patient_id ")


   maxageDF.createOrReplaceTempView("maxagetable")

   val ageDF= spark.sqlContext.sql("select patient_id, round(m_date/365) as age from maxagetable")

   ageDF.createOrReplaceTempView("agetable")

   //Calculate the Rank on Age of Patients
   val agerankDf=spark.sqlContext.sql("select patient_id,age,rank() over(order by age) min_rnk,rank() over(order by age desc) max_rnk from agetable ")

   agerankDf.createOrReplaceTempView("lasttable")

   //Maximum Age of patient
   val maxAgeDf= spark.sqlContext.sql("select * from lasttable where max_rnk=1 ")

   print("Maximum Age of patient :" + maxAgeDf.show())

   //Minimum Age of patient
   val minAgeDf= spark.sqlContext.sql("select * from lasttable where min_rnk=1")

   print("Minimum Age of patient " + minAgeDf.show())

   //Median Age of patient
   val medianAge=spark.sqlContext.sql("select percentile_approx(age,0.5) FROM lasttable")

   print("Median Age of patient :" + medianAge.show())


  }


}

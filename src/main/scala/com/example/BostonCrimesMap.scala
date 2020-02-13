package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
//import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
//import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{callUDF, lit, map, _}

object BostonCrimesMap extends App{

  Logger.getLogger("org").setLevel(Level.OFF)

  val t1 = System.nanoTime

  if (args.length != 3) {
    println("Incorrect arguments.")
    println("Usage: /path/to/jar {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder}")
    sys.exit(-1)
  }

  // Setup values from arguments
  val crime_file: String = args(0)
  val offense_codes_file: String = args(1)
  val out_folder: String = args(2)

//  val scFolder = "files/input/"
//  val crime_file: String = scFolder + "crime.csv"
//  val offense_codes_file: String = scFolder + "offense_codes.csv"
//  val out_folder: String = "files/output/"

  val spark = SparkSession.builder().master("local[*]").appName("BostonCrimesMap").getOrCreate()
  //val spark = SparkSession.builder().master("local").appName("BostonCrimesMap").getOrCreate()

  import spark.implicits._

  val crime: DataFrame = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(crime_file)
    .filter($"DISTRICT".isNotNull)

  val offense_codes: DataFrame = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(offense_codes_file)

    .withColumn("NAME_SPLIT", trim(split($"NAME", "-")(0)))
    .groupBy($"CODE")
    .agg(first($"NAME_SPLIT").alias("crime_type"))
    .orderBy($"CODE", $"crime_type")

  val offense_codes_broadcast: Broadcast[DataFrame] = spark.sparkContext.broadcast(offense_codes)

  val crime_new: DataFrame = crime
    .join(offense_codes_broadcast.value, $"CODE" === $"OFFENSE_CODE")
    .select("INCIDENT_NUMBER", "DISTRICT", "YEAR", "MONTH", "Lat", "Long", "crime_type")
    .na.fill(0.0)
    .cache

  val crime_out_1: DataFrame = crime_new
    .groupBy($"DISTRICT", $"YEAR", $"MONTH")
    .agg(count($"INCIDENT_NUMBER").alias("count_crimes"))

    .groupBy(($"DISTRICT").alias("district"))
    .agg(sum($"count_crimes").alias("crimes_total"),
      callUDF("percentile_approx", $"count_crimes", lit(0.5)).alias("crimes_monthly"))

  val window: WindowSpec = Window.partitionBy($"DISTRICT").orderBy($"count_crimes".desc)
  val crime_out_2: DataFrame = crime_new
    .groupBy($"DISTRICT", $"crime_type")
    .agg(count($"INCIDENT_NUMBER").alias("count_crimes"))

    .withColumn("rn", row_number().over(window))
    .filter($"rn" <= 3)
    .drop($"rn")

    .groupBy(($"DISTRICT").alias("district"))
    .agg(collect_list($"crime_type").alias("crime_type_list"))
    .withColumn("frequent_crime_types", array_join($"crime_type_list", ", "))
    .drop($"crime_type_list")

//  case class crime_temp(DISTRICT: String, crime_type: String, count_crimes: Long)
//  val crime_out_2: DataFrame = crime_new
//    .groupBy($"DISTRICT", $"crime_type")
//    .agg(count($"INCIDENT_NUMBER").alias("count_crimes"))
//    .as [crime_temp]
//
//    .groupByKey(x => x.DISTRICT)
//    .flatMapGroups{
//      case ( district, iter) => iter.toList.sortBy(x => -x.count_crimes).take(3)
//    }
//
//    .groupBy(($"DISTRICT").alias("district"))
//    .agg(collect_list($"crime_type").alias("crime_type_list"))
//    .withColumn("frequent_crime_types", array_join($"crime_type_list", ", "))
//    .drop($"crime_type_list")

  val crime_out_3: DataFrame = crime_new
    .groupBy(($"DISTRICT").alias("district"))
    .agg(mean($"Lat").alias("lat"),
      mean($"Long").alias("lng"))


  val crime_out: DataFrame = crime_out_1
    .join(crime_out_2, Seq("district"))
    .join(crime_out_3, Seq("district"))
    //.orderBy($"crimes_total".desc)
    .orderBy($"district")

  crime_out.show(false)

  crime_out.repartition(1)
    .write
    .option("header", "true")
    .mode("OVERWRITE")
    .parquet(out_folder)

  println("A parquet file created.")

  val duration = (System.nanoTime - t1) / 1e9d
  println("Duration: " + duration)

  spark.stop()

}

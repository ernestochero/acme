package com.sparkConfiguration

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkConfiguration {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  implicit val sc: SparkContext = spark.sparkContext
}

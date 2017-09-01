package com.ibm.crail.spark.tools.tpcds

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by atr on 01.09.17.
  */
trait TPCDataGenerator extends Serializable {
  def generate(
                sparkContext: SparkContext,
                name: String,
                partitions: Int,
                scaleFactor: String): RDD[String]
}
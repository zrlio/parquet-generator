package com.ibm.crail.spark.tools

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by atr on 24.10.16.
  */
object SparkTools {
  def countNumPartitions(spark: SparkSession, x:Dataset[_]): Long = {
    /* count how many partitions does it have */
    val accum = spark.sparkContext.longAccumulator("resultPartition")
    x.foreachPartition(p => accum.add(1))
    accum.value
  }
}

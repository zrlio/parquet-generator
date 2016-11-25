/*
 * parqgen: Parquet file generator for a given schema
 *
 * Author: Animesh Trivedi <atr@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.spark.tools

import com.ibm.crail.spark.tools.schema.{IntWithPayload, ParquetExample}
import com.ibm.crail.spark.tools.tpcds.Gen65.Gen65
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object ParqGen {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)


  def readAndReturnRows(spark: SparkSession, fileName: String, showRows: Int, expectedRows: Long): Unit = {
    /* now we read it back and check */
    val inputDF = spark.read.parquet(fileName)
    val items = inputDF.count()
    val partitions = SparkTools.countNumPartitions(spark, inputDF)
    if(showRows > 0) {
      inputDF.show(showRows)
    }
    println("----------------------------------------------------------------")
    println("RESULTS: file " + fileName + " contains " + items + " rows and makes " + partitions + " partitions when read")
    println("----------------------------------------------------------------")
    if(expectedRows > 0 ) {
      require(items == expectedRows,
        "Number of rows do not match, counted: " + items + " expected: " + expectedRows)
    }
  }

  def main(args: Array[String]) {
    val options = new ParseOptions()
    println(options.getBanner)
    println("concat arguments = " + foo(args))
    options.parse(args)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Parquet Generator")
      .getOrCreate()

    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", options.getCompressionType)

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    //FIXME: this is a stupid way of writing code, but I cannot template this with DS/scala
    if(options.getClassName.equalsIgnoreCase("ParquetExample")){
      /* some calculations */
      require(options.getRowCount % options.getTasks == 0, " Please set rowCount (-r) and tasks (-t) such that " +
        "rowCount%tasks == 0, currently, rows: " + options.getRowCount + " tasks " + options.getTasks)
      val rowsPerTask = options.getRowCount / options.getTasks
      val inputRDD = spark.sparkContext.parallelize(0 until options.getTasks, options.getTasks).flatMap { p =>
        val base = new ListBuffer[ParquetExample]()
        /* now we want to generate a loop and save the parquet file */
        for (a <- 0L until rowsPerTask) {
          base += ParquetExample(DataGenerator.getNextInt(options.getrRangeInt),
            DataGenerator.getNextLong,
            DataGenerator.getNextDouble,
            DataGenerator.getNextFloat,
            DataGenerator.getNextString(options.getVariableSize))
        }
        base
      }
      val outputDS = inputRDD.toDS().repartition(options.getPartitions)
      outputDS.write.format("parquet").mode(SaveMode.Overwrite).save(options.getOutput+".parquet")
      readAndReturnRows(spark, options.getOutput, options.getShowRows, options.getRowCount)
    } else if (options.getClassName.equalsIgnoreCase("IntWithPayload")){
      /* some calculations */
      require(options.getRowCount % options.getTasks == 0, " Please set rowCount (-r) and tasks (-t) such that " +
        "rowCount%tasks == 0, currently, rows: " + options.getRowCount + " tasks " + options.getTasks)
      val rowsPerTask = options.getRowCount / options.getTasks
      val inputRDD = spark.sparkContext.parallelize(0 until options.getTasks, options.getTasks).flatMap { p =>
        val base = new ListBuffer[IntWithPayload]()
        /* now we want to generate a loop and save the parquet file */
        for (a <- 0L until rowsPerTask) {
          base += IntWithPayload(DataGenerator.getNextInt(options.getrRangeInt),
            DataGenerator.getNextByteArray(options.getVariableSize))
        }
        base
      }
      val outputDS = inputRDD.toDS().repartition(options.getPartitions)
      outputDS.write.format("parquet").mode(SaveMode.Overwrite).save(options.getOutput+".parquet")
      readAndReturnRows(spark, options.getOutput, options.getShowRows, options.getRowCount)
    } else if (options.getClassName.equalsIgnoreCase("tpcds")){
      val gx = new Gen65(spark, options)
      /* no reading back here */
    } else {
      throw new Exception("Illegal class name: " + options.getClassName)
    }
    println("----------------------------------------------------------------")
    println("ParqGen: Data written out successfully to " + options.getOutput )
    println("----------------------------------------------------------------")
    spark.stop()
  }
}

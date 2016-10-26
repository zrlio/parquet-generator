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
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object ParqGen {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    val options = new ParseOptions()
    println(options.getBanner)
    println("concat arguments = " + foo(args))
    options.parse(args)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Parquet Generator")
      .config("spark.default.parallelism", options.getParalleism.toString)
      .getOrCreate()

    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", options.getCompressionType)

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    //FIXME: this is a stupid way of writing code, but I cannot template this with DS/scala
    if(options.getClassName.equalsIgnoreCase("ParquetExample")){
      val inputRDD = spark.sparkContext.parallelize(0 until options.getParalleism, options.getParalleism).flatMap { p =>
        val base = new ListBuffer[ParquetExample]()
        /* now we want to generate a loop and save the parquet file */
        for (a <- 0 until options.getRows) {
          base += ParquetExample(DataGenerator.getNextInt(options.getrRangeInt),
            DataGenerator.getNextLong,
            DataGenerator.getNextDouble,
            DataGenerator.getNextFloat,
            DataGenerator.getNextString(options.getVariableSize))
        }
        base
      }
      inputRDD.toDS().write.format("parquet").mode(SaveMode.Overwrite).save(options.getOutput)
    } else if (options.getClassName.equalsIgnoreCase("IntWithPayload")){
      val inputRDD = spark.sparkContext.parallelize(0 until options.getParalleism, options.getParalleism).flatMap { p =>
        val base = new ListBuffer[IntWithPayload]()
        /* now we want to generate a loop and save the parquet file */
        for (a <- 0 until options.getRows) {
          base += IntWithPayload(DataGenerator.getNextInt(options.getrRangeInt),
            DataGenerator.getNextByteArray(options.getVariableSize))
        }
        base
      }
      inputRDD.toDS().write.format("parquet").mode(SaveMode.Overwrite).save(options.getOutput)
    }else {
      throw new Exception("Illegal class name: " + options.getClassName)
    }
    println("ParqGen: Data written out sucessfully to " + options.getOutput)

    /* now we read it back and check */
    val inputDS = spark.read.parquet(options.getOutput)
    val items = inputDS.count()
    val partitions = SparkTools.countNumPartitions(spark, inputDS)
    if(options.getShowRows > 0) {
      inputDS.show(options.getShowRows)
    }
    println("----------------------------------------------------------------")
    println("RESULTS: file " + options.getOutput+ " contains " + items + " rows in " + partitions + " partitions")
    println("----------------------------------------------------------------")
    require(items == (options.getRows * options.getParalleism))
    spark.stop()
  }
}

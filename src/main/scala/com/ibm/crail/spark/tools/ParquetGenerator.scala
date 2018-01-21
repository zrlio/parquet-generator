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

import com.ibm.crail.spark.tools.schema._
import com.ibm.crail.spark.tools.tpcds.TPCDSTables
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object ParquetGenerator {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)


  def readAndReturnRows(spark: SparkSession, fileName: String, showRows: Int, expectedRows: Long): Unit = {
    if(showRows > 0) {
      /* now we read it back and check */
      val inputDF = spark.read.parquet(fileName)
      val items = inputDF.count()
      val partitions = SparkTools.countNumPartitions(spark, inputDF)
      inputDF.show(showRows)
      println("----------------------------------------------------------------")
      println("RESULTS: file " + fileName + " contains " + items + " rows and makes " + partitions + " partitions when read")
      println("----------------------------------------------------------------")
      if (expectedRows > 0) {
        require(items == expectedRows,
          "Number of rows do not match, counted: " + items + " expected: " + expectedRows)
      }
    }
  }

  private def doParquetExample(spark:SparkSession, rdd:RDD[Int], rowsPerTask:Int, options:ParseOptions):Unit = {
    val outputRdd = rdd.flatMap { p =>
      val base = new ListBuffer[ParquetExample]()
      /* now we want to generate a loop and save the parquet file */
      for (a <- 0L until rowsPerTask) {
        base += ParquetExample(DataGenerator.getNextInt(options.getRangeInt),
          DataGenerator.getNextLong,
          DataGenerator.getNextDouble,
          DataGenerator.getNextFloat,
          DataGenerator.getNextString(options.getVariableSize,
            options.getAffixRandom))
      }
      base
    }
    import spark.implicits._
    val outputDS = outputRdd.toDS().repartition(options.getPartitions)
    outputDS.write
      .options(options.getDataSinkOptions)
      .format(options.getOutputFileFormat)
      .mode(SaveMode.Overwrite)
      .save(options.getOutput)
    readAndReturnRows(spark, options.getOutput, options.getShowRows, options.getRowCount)
  }

  private def doIntWithPayload(spark:SparkSession, rdd:RDD[Int], rowsPerTask:Int, options:ParseOptions):Unit = {
    val outputRdd = rdd.flatMap { p =>
      val base = new ListBuffer[IntWithPayload]()
      /* now we want to generate a loop and save the parquet file */
      val size = options.getVariableSize
      for (a <- 0L until rowsPerTask) {
        base += IntWithPayload(DataGenerator.getNextInt(options.getRangeInt),
          DataGenerator.getNextByteArray(size,
            options.getAffixRandom))
      }
      base
    }
    import spark.implicits._
    val outputDS = outputRdd.toDS().repartition(options.getPartitions)
    outputDS.write
      .options(options.getDataSinkOptions)
      .format(options.getOutputFileFormat)
      .mode(SaveMode.Overwrite)
      .save(options.getOutput)
    readAndReturnRows(spark, options.getOutput, options.getShowRows, options.getRowCount)
  }

  private def doIntWithPayload2x(spark:SparkSession, rdd:RDD[Int], rowsPerTask:Int, options:ParseOptions):Unit = {
    val outputRdd = rdd.flatMap { p =>
      val base = new ListBuffer[IntWithPayload2x]()
      /* now we want to generate a loop and save the parquet file */
      val size = options.getVariableSize
      for (a <- 0L until rowsPerTask) {
        base += IntWithPayload2x(DataGenerator.getNextInt(options.getRangeInt),
          DataGenerator.getNextByteArray(DataGenerator.getNextInt(size),
            options.getAffixRandom),
          DataGenerator.getNextLong,
          DataGenerator.getNextByteArray(DataGenerator.getNextInt(size),
            options.getAffixRandom),
          DataGenerator.getNextDouble)
      }
      base
    }
    import spark.implicits._
    val outputDS = outputRdd.toDS().repartition(options.getPartitions)
    outputDS.write
      .options(options.getDataSinkOptions)
      .format(options.getOutputFileFormat)
      .mode(SaveMode.Overwrite)
      .save(options.getOutput)
    readAndReturnRows(spark, options.getOutput, options.getShowRows, options.getRowCount)
  }

  private def doIntOnly(spark:SparkSession, rdd:RDD[Int], rowsPerTask:Int, options:ParseOptions):Unit = {
    val outputRdd = rdd.flatMap { p =>
      val base = new Array[IntOnly](rowsPerTask)
      /* now we want to generate a loop and save the parquet file */
      val size = options.getVariableSize
      val baseInt = DataGenerator.getNextInt
      for (a <- 0 until rowsPerTask) {
        base(a) = IntOnly((baseInt + a).toInt)
      }
      base
    }
    import spark.implicits._
    val outputDS = outputRdd.toDS().repartition(options.getPartitions)
    outputDS.write
      .options(options.getDataSinkOptions)
      .format(options.getOutputFileFormat)
      .mode(SaveMode.Overwrite)
      .save(options.getOutput)
    readAndReturnRows(spark, options.getOutput, options.getShowRows, options.getRowCount)
  }


  private def doLongOnly(spark:SparkSession, rdd:RDD[Int], rowsPerTask:Int, options:ParseOptions):Unit = {
    val outputRdd = rdd.flatMap { p =>
      val base = new ListBuffer[LongOnly]()
      /* now we want to generate a loop and save the parquet file */
      val size = options.getVariableSize
      for (a <- 0L until rowsPerTask) {
        base += LongOnly(DataGenerator.getNextLong)
      }
      base
    }
    import spark.implicits._
    val outputDS = outputRdd.toDS().repartition(options.getPartitions)
    outputDS.write
      .options(options.getDataSinkOptions)
      .format(options.getOutputFileFormat)
      .mode(SaveMode.Overwrite)
      .save(options.getOutput)
    readAndReturnRows(spark, options.getOutput, options.getShowRows, options.getRowCount)
  }

  private def doDoubleOnly(spark:SparkSession, rdd:RDD[Int], rowsPerTask:Int, options:ParseOptions):Unit = {
    val outputRdd = rdd.flatMap { p =>
      val base = new ListBuffer[DoubleOnly]()
      /* now we want to generate a loop and save the parquet file */
      val size = options.getVariableSize
      for (a <- 0L until rowsPerTask) {
        base += DoubleOnly(DataGenerator.getNextDouble)
      }
      base
    }
    import spark.implicits._
    val outputDS = outputRdd.toDS().repartition(options.getPartitions)
    outputDS.write
      .options(options.getDataSinkOptions)
      .format(options.getOutputFileFormat)
      .mode(SaveMode.Overwrite)
      .save(options.getOutput)
    readAndReturnRows(spark, options.getOutput, options.getShowRows, options.getRowCount)
  }

  private def generateFromDefinedSchemas(options:ParseOptions, spark:SparkSession):Unit = {
    require(options.getRowCount % options.getTasks == 0, " Please set rowCount (-r) and tasks (-t) such that " +
      "rowCount%tasks == 0, currently, rows: " + options.getRowCount + " tasks " + options.getTasks)
    val rowsPerTask = options.getRowCount / options.getTasks
    val inputRDD = spark.sparkContext.parallelize(0 until options.getTasks, options.getTasks)
    require(rowsPerTask <= Integer.MAX_VALUE)
    if(options.getClassName.equalsIgnoreCase("ParquetExample")){
      doParquetExample(spark, inputRDD,      rowsPerTask.toInt, options)
    } else if(options.getClassName.equalsIgnoreCase("IntWithPayload")){
      doIntWithPayload(spark, inputRDD,      rowsPerTask.toInt, options)
    } else if(options.getClassName.equalsIgnoreCase("IntWithPayload2x")){
      doIntWithPayload2x(spark, inputRDD,      rowsPerTask.toInt, options)
    } else if (options.getClassName.equalsIgnoreCase("IntOnly")){
      doIntOnly(spark, inputRDD,      rowsPerTask.toInt, options)
    } else if (options.getClassName.equalsIgnoreCase("LongOnly")){
      doLongOnly(spark, inputRDD,      rowsPerTask.toInt, options)
    } else if (options.getClassName.equalsIgnoreCase("DoubleOnly")){
      doDoubleOnly(spark, inputRDD,      rowsPerTask.toInt, options)
    } else {
      throw new Exception(" Illegal class name " + options.getClassName)
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
      .enableHiveSupport()
      .getOrCreate()
    var warningString = new StringBuilder

    // for parquet it takes "uncompressed" and for ORC it takes "none" - huh !
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", options.getCompressionType)
    spark.sqlContext.setConf("orc.compress", "none")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    //FIXME: this is a stupid way of writing code, but I cannot template this with DS/scala
    if (options.getClassName.equalsIgnoreCase("tpcds")) {
      val tpcdsOptions = options.getTpcdsOptions
      val tables = new TPCDSTables(spark.sqlContext,
        tpcdsOptions.dsdgen_dir,
        tpcdsOptions.scale_factor,
        tpcdsOptions.useDoubleForDecimal,
        tpcdsOptions.seStringForDate)
      import scala.collection.JavaConverters._
      val immMap = options.getDataSinkOptions.asScala
      tables.genData(tpcdsOptions.data_location,
        tpcdsOptions.format,
        immMap.toMap,
        tpcdsOptions.overwrite,
        tpcdsOptions.partitionTables,
        tpcdsOptions.clusterByPartitionColumns,
        tpcdsOptions.filterOutNullPartitionValues,
        tpcdsOptions.tableFilter,
        tpcdsOptions.numPartitions,
        options.getTasks)
    } else {
      generateFromDefinedSchemas(options, spark)
    }
    println(warningString.mkString)
    println("----------------------------------------------------------------------------------------------")
    println("ParquetGenerator : " + options.getClassName + " data written out successfully to " + options.getOutput )
    println("----------------------------------------------------------------------------------------------")
    spark.stop()
  }
}

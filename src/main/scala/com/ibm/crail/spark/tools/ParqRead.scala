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

import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 11.10.16.
  */
object ParqRead {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    val options = new ParseOptions()
    println(options.getBanner)
    println("concat arguments = " + foo(args))
    if(args.length != 2) {
      System.err.println(" Tell me which file to read? ")
      System.exit(-1)
    }

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.default.parallelism", options.getParalleism.toString)
      .getOrCreate()

    val inputDS = spark.read.parquet(args(1))
    val items = inputDS.count()
    System.out.println("----------------------------------------------------------------")
    System.out.println(" Total number of rows are : " + items)
    inputDS.show()
    spark.stop()
  }
}

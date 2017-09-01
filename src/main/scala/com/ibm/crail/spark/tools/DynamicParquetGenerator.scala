package com.ibm.crail.spark.tools

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, Row, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by atr on 07.11.16.
  */
object DynamicParquetGenerator {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  //val clazz = ClassCompilerLoader.compileAndLoadClass(options.getClassFileName)
  val clazz = ClassCompilerLoader.compileAndLoadClass("/home/demo/DynamicExample.scala").asInstanceOf[Class[Product]]

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

    /* some calculations */
    require(options.getRowCount % options.getTasks == 0, " Please set rowCount (-r) and tasks (-t) such that " +
      "rowCount%tasks == 0, currently, rows: " + options.getRowCount + " tasks " + options.getTasks)
    val rowsPerTask = options.getRowCount / options.getTasks

    /* we want to dynamically compile and generate parquet objects */
    val obj = ObjectGenerator.makeNewObject(clazz)

    val dynamicEncoder = Encoders.product[Product]
    // Product encoder has this problem -- !!
    //  Exception in thread "main" scala.ScalaReflectionException: <none> is not a term
    //	at scala.reflect.api.Symbols$SymbolApi$class.asTerm(Symbols.scala:199)
    //	at scala.reflect.internal.Symbols$SymbolContextApiImpl.asTerm(Symbols.scala:84)
    //	at org.apache.spark.sql.catalyst.ScalaReflection$class.constructParams(ScalaReflection.scala:799)
    //	at org.apache.spark.sql.catalyst.ScalaReflection$.constructParams(ScalaReflection.scala:39)
    //	at org.apache.spark.sql.catalyst.ScalaReflection$class.getConstructorParameters(ScalaReflection.scala:788)
    //	at org.apache.spark.sql.catalyst.ScalaReflection$.getConstructorParameters(ScalaReflection.scala:39)
    //	at org.apache.spark.sql.catalyst.ScalaReflection$.org$apache$spark$sql$catalyst$ScalaReflection$$serializerFor(ScalaReflection.scala:570)
    //	at org.apache.spark.sql.catalyst.ScalaReflection$.serializerFor(ScalaReflection.scala:425)
    //	at org.apache.spark.sql.catalyst.encoders.ExpressionEncoder$.apply(ExpressionEncoder.scala:61)
    //	at org.apache.spark.sql.Encoders$.product(Encoders.scala:275)
    //	at com.ibm.crail.spark.tools.DynamicParquetGenerator$.main(DynamicParquetGenerator.scala:43)
    //	at com.ibm.crail.spark.tools.DynamicParquetGenerator.main(DynamicParquetGenerator.scala)
    //	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    //	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
    //	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    //	at java.lang.reflect.Method.invoke(Method.java:498)
    //	at org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:738)
    //	at org.apache.spark.deploy.SparkSubmit$.doRunMain$1(SparkSubmit.scala:187)
    //	at org.apache.spark.deploy.SparkSubmit$.submit(SparkSubmit.scala:212)
    //	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:126)
    //	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)

    //val dynamicEncoder = Encoders.kryo(clazz)
    println(" =================== " + dynamicEncoder.schema)

    val dynamicExprEncoder = dynamicEncoder.asInstanceOf[ExpressionEncoder[Product]]

    val inputRDD = spark.sparkContext.parallelize(0 until options.getTasks, options.getTasks).flatMap { p =>
      val base = new ListBuffer[Row]()
      /* now we want to generate a loop and save the parquet file */
      for (a <- 0L until rowsPerTask) {
        val obj = ObjectGenerator.makeNewObject(clazz)
        base+= Row(dynamicExprEncoder.toRow(obj))
      }
      base
    }

    //case class DynamicExample(randInt:Int, randLong: Long, randString:String)

    val fields = Seq(StructField("randInt", IntegerType), StructField("randLong", LongType), StructField("randString", StringType))

    val schema = StructType(fields)

    val outputDS = spark.createDataFrame(inputRDD, dynamicEncoder.schema)
    //val outputDS = inputRDD.toDS()

    outputDS.show(100)

    outputDS.write.format("parquet").mode(SaveMode.Overwrite).save(options.getOutput)


    println("----------------------------------------------------------------")
    println("ParqGen: Data written out successfully to " + options.getOutput + ", now counting ....")
    println("----------------------------------------------------------------")

    /* now we read it back and check */
    val inputDF = spark.read.parquet(options.getOutput)
    val items = inputDF.count()
    val partitions = SparkTools.countNumPartitions(spark, inputDF)
    if(options.getShowRows > 0) {
      inputDF.show(options.getShowRows)
    }
    println("----------------------------------------------------------------")
    println("RESULTS: file " + options.getOutput+ " contains " + items + " rows and makes " + partitions + " partitions when read")
    println("----------------------------------------------------------------")
    require(items == options.getRowCount,
      "Number of rows do not match, counted: " + items + " expected: " + options.getRowCount)
    spark.stop()
  }
}

package com.ibm.crail.spark.tools.tpcds

/**
  * Created by atr on 01.09.17.
  */

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.slf4j.LoggerFactory

//import scala.collection.immutable.Stream
//import scala.sys.process._

abstract class Tables(sqlContext: SQLContext, scaleFactor: String,
                      useDoubleForDecimal: Boolean = false, useStringForDate: Boolean = false)
  extends Serializable {

  def dataGenerator: TPCDataGenerator
  def tables: Seq[Table]

  private val log = LoggerFactory.getLogger(getClass)

  def sparkContext = sqlContext.sparkContext

  case class Table(name: String, partitionColumns: Seq[String], fields: StructField*) {
    val schema = StructType(fields)

    def nonPartitioned: Table = {
      Table(name, Nil, fields : _*)
    }

    /**
      *  If convertToSchema is true, the data from generator will be parsed into columns and
      *  converted to `schema`. Otherwise, it just outputs the raw data (as a single STRING column).
      *
      *  atr: For anything but the text format, the convertToSchema is true
      */
    def generateDataframe(convertToSchema: Boolean, numPartition: Int, numTasks:Int) = {
      val generatedData = dataGenerator.generate(sparkContext, name, numTasks, numPartition, scaleFactor)
      val rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          if (convertToSchema) {
            val values = l.split("\\|", -1).dropRight(1).map { v =>
              if (v.equals("")) {
                // If the string value is an empty string, we turn it to a null
                null
              } else {
                v
              }
            }
            Row.fromSeq(values)
          } else {
            Row.fromSeq(Seq(l))
          }
        }
      }

      if (convertToSchema) {
        val stringData =
          sqlContext.createDataFrame(
            rows,
            StructType(schema.fields.map(f => StructField(f.name, StringType))))

        val convertedData = {
          val columns = schema.fields.map { f =>
            col(f.name).cast(f.dataType).as(f.name)
          }
          stringData.select(columns: _*)
        }
        convertedData
      } else {
        sqlContext.createDataFrame(rows, StructType(Seq(StructField("value", StringType))))
      }
    }

    def convertTypes(): Table = {
      val newFields = fields.map { field =>
        val newDataType = field.dataType match {
          case decimal: DecimalType if useDoubleForDecimal => DoubleType
          case date: DateType if useStringForDate => StringType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, partitionColumns, newFields:_*)
    }

    def genData(location: String,
                format: String,
                formatOptions:Map[String, String],
                overwrite: Boolean,
                clusterByPartitionColumns: Boolean,
                filterOutNullPartitionValues: Boolean,
                numPartitions: Int,
                numTasks:Int,
                outOf:Int,
                totalTables:Int): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore

      //atr: here I already have a data frame !
      // the format is parquet, hence the condition is true
      val data = generateDataframe(format != "text", numPartitions, numTasks)

      /* atr: the logic below is for partitionColumn. If not then writer = data.write */
      val tempTableName = s"${name}_text"
      data.createOrReplaceTempView(tempTableName)

      val writer = if (partitionColumns.nonEmpty) {
        if (clusterByPartitionColumns) {
          val columnString = data.schema.fields.map { field =>
            field.name
          }.mkString(",")
          val partitionColumnString = partitionColumns.mkString(",")
          val predicates = if (filterOutNullPartitionValues) {
            partitionColumns.map(col => s"$col IS NOT NULL").mkString("WHERE ", " AND ", "")
          } else {
            ""
          }

          val query =
            s"""
               |SELECT
               |  $columnString
               |FROM
               |  $tempTableName
               |$predicates
               |DISTRIBUTE BY
               |  $partitionColumnString
            """.stripMargin
          val grouped = sqlContext.sql(query)
          println(s"Pre-clustering with partitioning columns with query $query.")
          log.info(s"Pre-clustering with partitioning columns with query $query.")
          grouped.write
        } else {
          data.write
        }
      } else {
        data.write
      }

      writer.format(format).options(formatOptions).mode(mode)
      if (partitionColumns.nonEmpty) {
        writer.partitionBy(partitionColumns : _*)
      }
      println(s"${outOf}/${totalTables} Generating table $name in database to $location with save mode $mode.")
      log.info(s"${outOf}/${totalTables} Generating table $name in database to $location with save mode $mode.")
      writer.save(location)
      sqlContext.dropTempTable(tempTableName)
    }

    def createExternalTable(location: String, format: String, databaseName: String,
                            overwrite: Boolean, discoverPartitions: Boolean = true): Unit = {

      val qualifiedTableName = databaseName + "." + name
      val tableExists = sqlContext.tableNames(databaseName).contains(name)
      if (overwrite) {
        sqlContext.sql(s"DROP TABLE IF EXISTS $databaseName.$name")
      }
      if (!tableExists || overwrite) {
        println(s"Creating external table $name in database $databaseName using data stored in $location.")
        log.info(s"Creating external table $name in database $databaseName using data stored in $location.")
        sqlContext.createExternalTable(qualifiedTableName, location, format)
      }
      if (partitionColumns.nonEmpty && discoverPartitions) {
        println(s"Discovering partitions for table $name.")
        log.info(s"Discovering partitions for table $name.")
        sqlContext.sql(s"ALTER TABLE $databaseName.$name RECOVER PARTITIONS")
      }
    }

    def createTemporaryTable(location: String, format: String): Unit = {
      println(s"Creating temporary table $name using data stored in $location.")
      log.info(s"Creating temporary table $name using data stored in $location.")
      sqlContext.read.format(format).load(location).registerTempTable(name)
    }

    def analyzeTable(databaseName: String, analyzeColumns: Boolean = false): Unit = {
      println(s"Analyzing table $name.")
      log.info(s"Analyzing table $name.")
      sqlContext.sql(s"ANALYZE TABLE $databaseName.$name COMPUTE STATISTICS")
      if (analyzeColumns) {
        val allColumns = fields.map(_.name).mkString(", ")
        println(s"Analyzing table $name columns $allColumns.")
        log.info(s"Analyzing table $name columns $allColumns.")
        sqlContext.sql(s"ANALYZE TABLE $databaseName.$name COMPUTE STATISTICS FOR COLUMNS $allColumns")
      }
    }
  }

  /* atr this is the main entry function to generate data */
  def genData(
               location: String,
               format: String,
               formatOptions:Map[String, String],
               overwrite: Boolean,
               partitionTables: Boolean,
               clusterByPartitionColumns: Boolean,
               filterOutNullPartitionValues: Boolean,
               tableFilter: String = "",
               numPartitions: Int,
               numTasks:Int): Unit = {
    /* atr: set of there is paritionColumn or not. The logic below just will override the definition that was
     set in TPCDSTables Seq(Table(...)).
     I am not yet completely sure what does this mean to partiion a column
      */
    var tablesToBeGenerated = if (partitionTables) {
      tables
    } else {
      tables.map(_.nonPartitioned)
    }
    /* atr: if a particular table name to be dropped or not */
    if (!tableFilter.isEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter(_.name == tableFilter)
      if (tablesToBeGenerated.isEmpty) {
        throw new RuntimeException("Bad table name filter: " + tableFilter)
      }
    }
    val totalTables = tablesToBeGenerated.size
    var i = 1
    tablesToBeGenerated.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      /* atr: for each table generate data */
      table.genData(tableLocation, format, formatOptions, overwrite, clusterByPartitionColumns,
        filterOutNullPartitionValues, numPartitions, numTasks, i, totalTables)
      i+=1
    }
  }

  def createExternalTables(location: String, format: String, databaseName: String,
                           overwrite: Boolean, discoverPartitions: Boolean, tableFilter: String = ""): Unit = {

    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }

    sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    filtered.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.createExternalTable(tableLocation, format, databaseName, overwrite, discoverPartitions)
    }
    sqlContext.sql(s"USE $databaseName")
    println(s"The current database has been set to $databaseName.")
    log.info(s"The current database has been set to $databaseName.")
  }

  def createTemporaryTables(location: String, format: String, tableFilter: String = ""): Unit = {
    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }
    filtered.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.createTemporaryTable(tableLocation, format)
    }
  }

  def analyzeTables(databaseName: String, analyzeColumns: Boolean = false, tableFilter: String = ""): Unit = {
    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }
    filtered.foreach { table =>
      table.analyzeTable(databaseName, analyzeColumns)
    }
  }
}

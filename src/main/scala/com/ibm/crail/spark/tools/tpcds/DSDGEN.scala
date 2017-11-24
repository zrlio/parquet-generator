package com.ibm.crail.spark.tools.tpcds

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by atr on 24.11.17.
  */
class DSDGEN(dsdgenDir: String) extends TPCDataGenerator {
  val dsdgen = s"$dsdgenDir/dsdgen"

  def generate(sparkContext: SparkContext, name: String, numTasks:Int, numPartition: Int, scaleFactor: String): RDD[String] = {
    val generatedData = {
      sparkContext.parallelize(1 to numTasks, numTasks).flatMap { i =>
        val localToolsDir = if (new java.io.File(dsdgen).exists) {
          dsdgenDir
        } else if (new java.io.File(s"/$dsdgen").exists) {
          s"/$dsdgenDir"
        } else {
          sys.error(s"Could not find dsdgen at $dsdgen or /$dsdgen. Run install")
        }

        // Note: RNGSEED is the RNG seed used by the data generator. Right now, it is fixed to 100.
        val parallel = if (numTasks > 1) s"-parallel $numTasks -child $i" else ""
        val commands = Seq(
          "bash", "-c",
          s"cd $localToolsDir && ./dsdgen -table $name -filter Y -scale $scaleFactor -RNGSEED 100 $parallel")
        println(commands)
        BlockingLineStream(commands)
      }
    }
    generatedData.setName(s"$name, sf=$scaleFactor, strings")
    generatedData.repartition(numTasks)
  }
}
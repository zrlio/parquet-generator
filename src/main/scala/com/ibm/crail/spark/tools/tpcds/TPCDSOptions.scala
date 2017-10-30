package com.ibm.crail.spark.tools.tpcds

/**
  * Created by atr on 01.09.17.
  */
case class TPCDSOptions(var dsdgen_dir:String = "/home/atr/zrl/external/github/databricks/tpcds-kit/tools/",
                        var scale_factor:String = "1",
                        var data_location:String = "file:/data/tpcds-F1",
                        var format:String = "parquet",
                        var formatOptions:Map[String, String] = Map[String, String](),
                        var overwrite:Boolean = true,
                        var partitionTables:Boolean = false,
                        var clusterByPartitionColumns:Boolean = false,
                        var filterOutNullPartitionValues:Boolean = false,
                        var tableFilter: String = "",
                        var numPartitions: Int = 4,
                        var useDoubleForDecimal:Boolean = false,
                        var seStringForDate: Boolean = false){
}

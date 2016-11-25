package com.ibm.crail.spark.tools.tpcds.Gen65

import com.ibm.crail.spark.tools.{DataGenerator, ParseOptions}
import com.ibm.crail.spark.tools.schema.TPCDS
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by atr on 25.11.16.
  */
case class Gen65(spark: SparkSession, options: ParseOptions) {

  import spark.implicits._

  /* this class is suppose to generate data for 4 tables */

  /* step 1: make function that generate valid data sets */

  def nextRandYear() : Int = {
    /* we generate years between (2001) or (not 2001) */
    if (DataGenerator.getNextInt %2 == 0)
      2001
    else
      2000
  }

  def nextString() : String = {
    /* we generate strings between [0, variableSize) */
    DataGenerator.getNextString(DataGenerator.getNextInt(options.getVariableSize))
  }

  def nextInt() : Int = {
    /* we generate ints between [0, and -R) */
    DataGenerator.getNextInt(options.getrRangeInt())
  }

  def nextFloat() : Float = {
    DataGenerator.getNextFloat
  }

  /* this is where we generate store */
  var rowsPerTask = options.getQ65Map.get("store") / options.getTasks
  val storeRDD = spark.sparkContext.parallelize(0 until options.getTasks, options.getTasks).flatMap { p =>
    val base = new ListBuffer[TPCDS.store]()
    /* now we want to generate a loop and save the parquet file */
    for (a <- 0L until rowsPerTask) {
      base += TPCDS.store(s_store_sk = nextInt() /* this is used in matching */,
        s_store_id= nextString(),
        s_rec_start_date = nextString(),
        s_rec_end_date = nextString(),
        s_closed_date_sk = nextInt(),
        s_store_name = nextString(),
        s_number_employees = nextInt(),
        s_floor_space = nextInt(),
        s_hours = nextString(),
        s_manager = nextString(),
        s_market_id = nextInt(),
        s_geography_class =  nextString(),
        s_market_desc = nextString(),
        s_market_manager = nextString(),
        s_division_id = nextInt(),
        s_division_name = nextString(),
        s_company_id = nextInt(),
        s_company_name= nextString(),
        s_street_number= nextString(),
        s_street_name= nextString(),
        s_street_type= nextString(),
        s_suite_number= nextString(),
        s_city= nextString(),
        s_county= nextString(),
        s_state= nextString(),
        s_zip= nextString(),
        s_country= nextString(),
        s_gmt_offset = nextFloat(),
        s_tax_precentage = nextFloat())
    }
    base
  }
  val storeDS = storeRDD.toDS().repartition(options.getPartitions)
  storeDS.write.format("parquet").mode(SaveMode.Overwrite).save(options.getOutput+"/store")

  /* -------------------------------------------------------------------------------------- */
  rowsPerTask = options.getQ65Map.get("date_dim") / options.getTasks
  val date_dimRDD = spark.sparkContext.parallelize(0 until options.getTasks, options.getTasks).flatMap { p =>
    val base = new ListBuffer[TPCDS.date_dim]()
    /* now we want to generate a loop and save the parquet file */
    for (a <- 0L until rowsPerTask) {
      base += TPCDS.date_dim(d_date_sk= nextInt(),
        d_date_id= nextString(),
        d_date= nextString(),
        d_month_seq= nextInt(),
        d_week_seq= nextInt(),
        d_quarter_seq= nextInt(),
        d_year= nextInt(),
        d_dow= nextInt(),
        d_moy= nextInt(),
        d_dom= nextInt(),
        d_qoy= nextInt(),
        d_fy_year= nextInt(),
        d_fy_quarter_seq= nextInt(),
        d_fy_week_seq= nextInt(),
        d_day_name= nextString(),
        d_quarter_name= nextString(),
        d_holiday= nextString(),
        d_weekend= nextString(),
        d_following_holiday= nextString(),
        d_first_dom= nextInt(),
        d_last_dom= nextInt(),
        d_same_day_ly= nextInt(),
        d_same_day_lq= nextInt(),
        d_current_day= nextString(),
        d_current_week= nextString(),
        d_current_month= nextString(),
        d_current_quarter= nextString(),
        d_current_year= nextString())
    }
    base
  }
  val date_dimDS = date_dimRDD.toDS().repartition(options.getPartitions)
  date_dimDS.write.format("parquet").mode(SaveMode.Overwrite).save(options.getOutput+"/date_dim")

  /* -------------------------------------------------------------------------------------- */
  rowsPerTask = options.getQ65Map.get("item") / options.getTasks
  val itemRDD = spark.sparkContext.parallelize(0 until options.getTasks, options.getTasks).flatMap { p =>
    val base = new ListBuffer[TPCDS.item]()
    /* now we want to generate a loop and save the parquet file */
    for (a <- 0L until rowsPerTask) {
      base += TPCDS.item(  i_item_sk                 = nextInt(),
        i_item_id                 = nextString(),
        i_rec_start_date          = nextString(),
        i_rec_end_date            = nextString(),
        i_item_desc               = nextString(),
        i_current_price           = nextFloat(),
        i_wholesale_cost          = nextFloat(),
        i_brand_id                = nextInt(),
        i_brand                   = nextString(),
        i_class_id                = nextInt(),
        i_class                   = nextString(),
        i_category_id             = nextInt(),
        i_category                = nextString(),
        i_manufact_id             = nextInt(),
        i_manufact                = nextString(),
        i_size                    = nextString(),
        i_formulation             = nextString(),
        i_color                   = nextString(),
        i_units                   = nextString(),
        i_container               = nextString(),
        i_manager_id              = nextInt(),
        i_product_name            = nextString())
    }
    base
  }
  val itemDS = itemRDD.toDS().repartition(options.getPartitions)
  itemDS.write.format("parquet").mode(SaveMode.Overwrite).save(options.getOutput+"/item")

  /* -------------------------------------------------------------------------------------- */
  rowsPerTask = options.getQ65Map.get("store_sales") / options.getTasks
  val store_salesRDD = spark.sparkContext.parallelize(0 until options.getTasks, options.getTasks).flatMap { p =>
    val base = new ListBuffer[TPCDS.store_sales]()
    /* now we want to generate a loop and save the parquet file */
    for (a <- 0L until rowsPerTask) {
      base += TPCDS.store_sales(ss_sold_date_sk= nextInt(),
        ss_sold_time_sk= nextInt(),
        ss_item_sk= nextInt(),
        ss_customer_sk= nextInt(),
        ss_cdemo_sk= nextInt(),
        ss_hdemo_sk= nextInt(),
        ss_addr_sk= nextInt(),
        ss_store_sk= nextInt(),
        ss_promo_sk= nextInt(),
        ss_ticket_number= nextInt(),
        ss_quantity= nextInt(),
        ss_wholesale_cost = nextFloat(),
        ss_list_price= nextFloat(),
        ss_sales_price= nextFloat(),
        ss_ext_discount_amt= nextFloat(),
        ss_ext_sales_price= nextFloat(),
        ss_ext_wholesale_cost= nextFloat(),
        ss_ext_list_price= nextFloat(),
        ss_ext_tax= nextFloat(),
        ss_coupon_amt= nextFloat(),
        ss_net_paid= nextFloat(),
        ss_net_paid_inc_tax= nextFloat(),
        ss_net_profit= nextFloat())
    }
    base
  }
  val store_salesDS = store_salesRDD.toDS().repartition(options.getPartitions)
  store_salesDS.write.format("parquet").mode(SaveMode.Overwrite).save(options.getOutput+"/store_sales")
}

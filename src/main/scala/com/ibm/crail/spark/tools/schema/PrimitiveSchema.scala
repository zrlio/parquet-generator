package com.ibm.crail.spark.tools.schema

/**
  * Created by atr on 28.11.17.
  */
trait  PrimitiveSchema

case class LongOnly(value:Long) extends PrimitiveSchema
case class IntOnly(value:Int) extends PrimitiveSchema
case class DoubleOnly(value:Double) extends PrimitiveSchema
case class FloatOnly(value:Float) extends PrimitiveSchema
case class CharOnly(value:Char) extends PrimitiveSchema

case class TemplateSchema[T](value:T) extends PrimitiveSchema

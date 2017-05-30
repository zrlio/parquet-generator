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

import scala.util.Random

/**
  * Created by atr on 10/5/16.
  */
object DataGenerator extends Serializable {
  val random = new Random(System.nanoTime())

  /* affix payload variables */
  var affixStringBuilder:StringBuilder = null
  var affixByteArray:Array[Byte] = null

  def getNextString(size: Int, affix: Boolean):String = {
    if(affix){
      this.synchronized {
        if (affixStringBuilder == null) {
          affixStringBuilder = new StringBuilder(
            random.alphanumeric.take(size).mkString)
        }
      }
      /* just randomly change 1 byte - this is to make sure parquet
      * does not ignore the data */
      affixStringBuilder.setCharAt(random.nextInt(size),
        random.nextPrintableChar())
      affixStringBuilder.mkString
    } else {
      random.alphanumeric.take(size).mkString
    }
  }

  def getNextByteArray(size: Int, affix: Boolean):Array[Byte] = {
    val toReturn = new Array[Byte](size)
    if(!affix){
      /* if not affix, then return completely new values in a new array */
      random.nextBytes(toReturn)
    } else {
      this.synchronized{
        if(affixByteArray == null){
          affixByteArray = new Array[Byte](size)
          /* initialize */
          random.nextBytes(affixByteArray)
        }
      }
      /* just randomly change 1 byte - this is to make sure parquet
      * does not ignore the data - char will be casted to byte */
      affixByteArray(random.nextInt(size)) = random.nextPrintableChar().toByte
      /* now we copy affix array */
      Array.copy(affixByteArray, 0, toReturn, 0, size)
    }
    toReturn
  }

  def getNextInt:Int = {
    random.nextInt()
  }

  def getNextInt(max:Int):Int = {
    random.nextInt(max)
  }

  def getNextLong:Long= {
    random.nextLong()
  }

  def getNextDouble:Double= {
    random.nextDouble()
  }

  def getNextFloat: Float = {
    random.nextFloat()
  }

  def getNextValue(s:String, size: Int, affix:Boolean): String ={
    getNextString(size, affix)
  }

  def getNextValue(i:Int): Int = {
    getNextInt
  }

  def getNextValue(d:Double): Double = {
    getNextDouble
  }

  def getNextValue(l:Long): Long = {
    getNextLong
  }

  def getNextValueClass(cls: Any): Any = {
    val data = cls match {
      case _:String => DataGenerator.getNextString(10, false)
      case _ => throw new Exception("Data type not supported: ")
    }
    data
  }

  def getNextValuePrimitive(typeX: Class[_]): Any = {
    val data = typeX match {
      case java.lang.Integer.TYPE => DataGenerator.getNextInt
      case java.lang.Long.TYPE => DataGenerator.getNextLong
      case java.lang.Float.TYPE => DataGenerator.getNextFloat
      case java.lang.Double.TYPE => DataGenerator.getNextDouble
      case _ => println("others")
    }
    data
  }

  def getNextValue(typeX: Class[_]) : Any = {
    if(typeX.isPrimitive)
      getNextValuePrimitive(typeX)
    else
      getNextValueClass(typeX.newInstance())
  }

  /* another example - this is what I wanted in the first place */
  def f[T](v: T) = v match {
    case _: Int    => "Int"
    case _: String => "String"
    case _         => "Unknown"
  }

}

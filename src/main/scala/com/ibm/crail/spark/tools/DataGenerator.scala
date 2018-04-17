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

import java.util.concurrent.ThreadLocalRandom

import scala.util.Random

/**
  * Created by atr on 10/5/16.
  */
object DataGenerator extends Serializable {
  val scalaRandom = new Random(System.nanoTime())

  //val ThreadLocalRandom.current() = ThreadLocalRandom.current()
  //ThreadLocalRandom.current().setSeed(System.nanoTime())

  /* affix payload variables */
  var affixStringBuilder:StringBuilder = null
  val baseRandomArray:Array[Byte] = new Array[Byte](1024*1024)
  ThreadLocalRandom.current().nextBytes(this.baseRandomArray)

  def getNextString(size: Int, affix: Boolean):String = {
    if(affix){
      this.synchronized {
        if (affixStringBuilder == null) {
          affixStringBuilder = new StringBuilder(
            scalaRandom.alphanumeric.take(size).mkString)
        }
      }
      /* just randomly change 1 byte - this is to make sure parquet
      * does not ignore the data */
      affixStringBuilder.setCharAt(ThreadLocalRandom.current().nextInt(size),
        scalaRandom.nextPrintableChar())
      affixStringBuilder.mkString
    } else {
      scalaRandom.alphanumeric.take(size).mkString
    }
  }

  def getNextByteArray(size: Int, affix: Boolean):Array[Byte] = {
    val toReturn = new Array[Byte](size)
    if(!affix){
      /* if not affix, then return completely new values in a new array */
      ThreadLocalRandom.current().nextBytes(toReturn)
    } else {
      /* now we need to fill out the passed array from our source */
      var leftBytes = toReturn.length
      var srcOffset = 0
      var dstOffset = 0
      while(leftBytes > 0){
        val toCopy = Math.min(leftBytes, this.baseRandomArray.length)
        /* just randomly change 1 byte - this is to make sure parquet
      * does not ignore the data - char will be casted to byte */
        this.baseRandomArray(ThreadLocalRandom.current().nextInt(toCopy)) = scalaRandom.nextPrintableChar().toByte
        Array.copy(this.baseRandomArray, srcOffset, toReturn, dstOffset, toCopy)
        dstOffset+=toCopy
        srcOffset+= toCopy
        if (srcOffset == this.baseRandomArray.length)
          srcOffset = 0
        leftBytes-=toCopy
      }
    }
    toReturn
  }

  def getNextInt:Int = {
    ThreadLocalRandom.current().nextInt()
  }

  def getNextInt(max:Int):Int = {
    ThreadLocalRandom.current().nextInt(max)
  }

  def getNextLong:Long= {
    ThreadLocalRandom.current().nextLong()
  }

  def getNextDouble:Double= {
    ThreadLocalRandom.current().nextDouble()
  }

  def getNextFloat: Float = {
    ThreadLocalRandom.current().nextFloat()
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

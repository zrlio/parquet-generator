package com.ibm.crail.spark.tools

import java.nio.ByteBuffer

import scala.collection.mutable.StringBuilder
import scala.util.Random

/**
  * Created by atr on 28.11.17.
  */
class DataGeneratorV2 extends Serializable {
  val random = new Random(System.nanoTime())

  /* affix payload variables */
  var affixStringBuilder:StringBuilder = null
  var affixByteArray:Array[Byte] = null

  val poolOfRandomStuff:Array[Byte] = new Array[Byte](1024*1024)
  random.nextBytes(poolOfRandomStuff)
  val bb:ByteBuffer = ByteBuffer.wrap(poolOfRandomStuff)
  bb.clear()

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
    if(bb.remaining() < 4) {
      bb.clear()
    }
    bb.getInt()
  }

  def getNextInt(max:Int):Int = {
    getNextInt % max
  }

  def getNextLong:Long= {
    if(bb.remaining() < 8) {
      bb.clear()
    }
    bb.getLong
  }

  def getNextDouble:Double= {
    if(bb.remaining() < 8) {
      bb.clear()
    }
    bb.getDouble
  }

  def getNextFloat: Float = {
    if(bb.remaining() < 4)
      bb.clear()
    bb.getFloat()
  }

  def getNextValue(s:String, size: Int, affix:Boolean): String ={
    getNextString(size, affix)
  }
}


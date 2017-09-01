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

import org.apache.spark.sql.types.StructType

import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}

/**
  * Created by atr on 06.10.16.
  */
object ObjectGenerator {

  def getClassReference(options: ParseOptions) : Class[_] = {
    if(options.getClassFileName != null){
      ClassCompilerLoader.compileAndLoadClass(options.getClassFileName)
    } else {
      /* load from the environment */
      Class.forName(options.getClassName)
    }
  }

  def showContructor(clazz: Class[_]) = {
    val consts = clazz.getDeclaredConstructors
    for (ctor <- consts) {
      var output = clazz.getName + "( "
      val pType = ctor.getParameterTypes
      for (i <- 0 until pType.length) {
        output+=pType(i).getName
        if(i != pType.length -1)
          output+=", "
      }
      output+=")"
      System.out.println(output)
    }
  }

  def getSparkSchema(clazz: Class[Product]): StructType = {
    val consts = clazz.getDeclaredConstructors
    if(consts.length != 1){
      throw new Exception("There are " + consts.length + " constructors. Please make a case class")
    }
    val pType = consts(0).getParameterTypes
    for (i <- 0 until pType.length) {
      System.out.println(" var: " + pType(i).getName + " type: " + pType(i).getTypeName)
    }
    null
  }

  def caseClassParamsOf[T: TypeTag]: ListMap[String, Type] = {
    val tpe = typeOf[T]
    val constructorSymbol = tpe.decl(termNames.CONSTRUCTOR)
    val defaultConstructor =
      if (constructorSymbol.isMethod) constructorSymbol.asMethod
      else {
        val ctors = constructorSymbol.asTerm.alternatives
        ctors.map { _.asMethod }.find { _.isPrimaryConstructor }.get
      }

    ListMap[String, Type]() ++ defaultConstructor.paramLists.reduceLeft(_ ++ _).map {
      sym => sym.name.toString -> tpe.member(sym.name).asMethod.returnType
    }
  }


  def makeNewObject(clazz: Class[Product]): Product = {
    val consts = clazz.getDeclaredConstructors
    if(consts.length != 1){
      throw new Exception("There are " + consts.length + " constructors. Please make a case class")
    }
    val pType = consts(0).getParameterTypes
    var args:ListBuffer[Any] = new ListBuffer[Any]
    for (i <- 0 until pType.length) {
      args = args++Seq(DataGenerator.getNextValue(pType(i)))
    }
    // This is the magic line
    //scala> Foo.getClass.getMethods.find(x => x.getName == "apply" && x.isBridge).get.invoke(Foo, params map (_.asInstanceOf[AnyRef]): _*).asInstanceOf[Foo]
    val myObject = consts(0).newInstance(args map (_.asInstanceOf[AnyRef]): _*)
    //System.out.println("object allocation " + myObject)
    /* because this is a case class */
    myObject.asInstanceOf[Product]
  }
}

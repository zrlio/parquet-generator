/*
 * parqgen: Parquet file generator for a given schema
 *
 * Credits: https://gist.github.com/metasim/7492509
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

package com.ibm.crail.spark.tools;

import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.internal.util.AbstractFileClassLoader;
import scala.reflect.internal.util.BatchSourceFile;
import scala.reflect.io.AbstractFile;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;
import scala.tools.nsc.GenericRunnerSettings;
import scala.tools.nsc.interpreter.IMain;
import scala.tools.nsc.settings.MutableSettings;

import java.io.File;
import java.util.Collections;

/**
 * Created by atr on 07.10.16.
 */
public class ClassCompilerLoader {
    static private class ErrorHandler extends AbstractFunction1<String, BoxedUnit> {
        @Override
        public BoxedUnit apply(String message) {
            System.err.println("Interpreter error: " + message);
            return BoxedUnit.UNIT;
        }
    }

    static public Class<?> compileAndLoadClass(String fileLocation) throws ClassNotFoundException {
        try {
            GenericRunnerSettings settings = new GenericRunnerSettings(new ErrorHandler());
            ((MutableSettings.BooleanSetting) settings.usejavacp()).v_$eq(true);
            IMain interpreter = new IMain(settings);
            File source = new File(fileLocation);
            Iterable sources = Collections.singletonList(new BatchSourceFile(AbstractFile.getFile(source.toString())));
            Seq seq = JavaConversions.iterableAsScalaIterable(sources).toSeq();
            interpreter.compileSources(seq);
            AbstractFileClassLoader interpreterClassLoader = interpreter.classLoader();
            AbstractFile root = interpreterClassLoader.root();
            Iterator<AbstractFile> itr = root.iterator();
            while(itr.hasNext()) {
                AbstractFile classFileName = itr.next();
                String name = classFileName.name().replace(".class", "");
                /* compile code contains these classes YourClass.class , $repl_$init.class, YourClass.class */
                if(!name.contains("$")) {
                    System.out.println("Found a class : " + name);
                    Class<?> clazz = interpreterClassLoader.findClass(name);
                    return clazz;
                }
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
        throw new ClassNotFoundException("No valid class found in the file : " + fileLocation);
    }
}

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

package com.ibm.crail.spark.tools;

import org.apache.commons.cli.*;


/**
 * Created by atr on 9/30/16.
 */
public class ParseOptions {
    private Options options;
    private int rows;
    private String output;
    private String classFileName;
    private String className;
    private int paralleism;
    private String compressionType;
    private int variableSize;
    private String banner;

    public ParseOptions(){
        this.rows = 10;
        this.output = "/ParqGenOutput.parquet";
        this.classFileName = null;
        this.className ="com.ibm.crail.spark.toolsX.schema.ParquetExample";
        this.paralleism = 1;
        this.compressionType = "uncompressed";
        this.variableSize = 100;

        options = new Options();
        options.addOption("h", "help", false, "show help");
        options.addOption("r", "rows", true, "<int> number of rows (default: " + this.rows+")");
        options.addOption("c", "case", true, "case class schema currently supported are: \n" +
                "                             ParquetExample (default), IntWithPayload. \n" +
                "                             These classes are in ./schema/ in src.");
        options.addOption("f", "caseFile", true, "<String> case class file to compile and load (NYI)");
        options.addOption("o", "output", true, "<String> the output file name (default: " + this.output+")");
        options.addOption("p", "parallelism", true, "<int> default parallelism (NYI)");
        options.addOption("s", "size", true, "<int> any variable payload size, string or payload in IntPayload (default: " + this.variableSize+")");
        options.addOption("C", "compress", true, "<String> compression type, valid values are: uncompressed, snappy, gzip, lzo (default: "
                + this.compressionType+")");

        String banner2 = "(_____ \\                / _____)            \n" +
                " _____) )___  ____ ____| /  ___  ____ ____  \n" +
                "|  ____/ _  |/ ___) _  | | (___)/ _  )  _ \\ \n" +
                "| |   ( ( | | |  | | | | \\____/( (/ /| | | |\n" +
                "|_|    \\_||_|_|   \\_|| |\\_____/ \\____)_| |_|";

        String banner3 = "| ___ \\             |  __ \\           \n" +
                "| |_/ /_ _ _ __ __ _| |  \\/ ___ _ __  \n" +
                "|  __/ _` | '__/ _` | | __ / _ \\ '_ \\ \n" +
                "| | | (_| | | | (_| | |_\\ \\  __/ | | |\n" +
                "\\_|  \\__,_|_|  \\__, |\\____/\\___|_| |_|\n" +
                "                  | |                 \n" +
                "                  |_|                 ";

        String banner1 = " ____                  ____            \n" +
                "|  _ \\ __ _ _ __ __ _ / ___| ___ _ __  \n" +
                "| |_) / _` | '__/ _` | |  _ / _ \\ '_ \\ \n" +
                "|  __/ (_| | | | (_| | |_| |  __/ | | |\n" +
                "|_|   \\__,_|_|  \\__, |\\____|\\___|_| |_|\n" +
                "                   |_|                 \n";

        this.banner = banner1;
    }

    public String getOutput(){
        return this.output;
    }

    public String getClassFileName(){
        return this.classFileName;
    }

    public String getClassName() {
        return this.className;
    }

    public int getRows(){
        return this.rows;
    }

    public int getParalleism(){
        return this.paralleism;
    }

    public String getCompressionType(){
        return this.compressionType;
    }

    public int getVariableSize(){
        return this.variableSize;
    }

    public String getBanner(){
        return  this.banner;
    }

    private int getMatchingIndex(String[] options, String name) {
        int i;
        for(i = 0; i < options.length; i++)
            if (name.equalsIgnoreCase(options[i])){
                return i;
            }
        throw  new IllegalArgumentException(name + " not found in " + options);
    }

    public void show_help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Parqgen", options);
    }

    public void parse(String[] args) {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        boolean cset = false;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                show_help();
                System.exit(0);
            }

            if (cmd.hasOption("r")) {
                this.rows = Integer.parseInt(cmd.getOptionValue("r").trim());
            }

            if (cmd.hasOption("o")) {
                this.output = cmd.getOptionValue("o").trim();
            }

            if (cmd.hasOption("f")) {
                this.classFileName = cmd.getOptionValue("f").trim();
            }

            if (cmd.hasOption("c")) {
                this.className = cmd.getOptionValue("c").trim();
                cset = true;
            }

            if (cmd.hasOption("C")) {
                this.compressionType = cmd.getOptionValue("C").trim();
            }

            if (cmd.hasOption("s")) {
                this.variableSize = Integer.parseInt(cmd.getOptionValue("s").trim());
            }

        } catch (ParseException e) {
            System.err.println("Failed to parse command line properties" + e);
            show_help();
            System.exit(-1);
        }
        /* do some sanity checks */
        if(this.classFileName != null && (this.className != null && cset) ){
            System.err.println("You cannot define both -f and -c. Please use one");
            show_help();
            System.exit(-1);
        }
        /* this will never happen as this.className is defined to a default class */
        if(this.classFileName == null && this.className == null){
            System.err.println("You have to define atleast one class, use either -f XOR -c.");
            show_help();
            System.exit(-1);
        }
    }
}

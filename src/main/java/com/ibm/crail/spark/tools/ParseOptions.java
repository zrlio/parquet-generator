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

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by atr on 9/30/16.
 */
public class ParseOptions implements Serializable {
    private Options options;
    private long rowCount;
    private String output;
    private String classFileName;
    private String className;
    private int tasks;
    private int partitions;
    private String compressionType;
    private int variableSize;
    private String banner;
    private int showRows;
    private int rangeInt;
    private HashMap<String, Long> q65Map;


    public ParseOptions(){
        this.rowCount = 10;
        this.output = "/ParqGenOutput.parquet";
        this.classFileName = null;
        this.className ="ParquetExample";
        this.tasks = 1;
        this.partitions = 1;
        this.compressionType = "uncompressed";
        this.variableSize = 100;
        this.showRows = 0;
        this.rangeInt = Integer.MAX_VALUE;
        this.q65Map = new HashMap<>(4);

        /* these are the numbers for a 1TB run */
        //q65Map.put("store", 1002L);
        //q65Map.put("date_dim", 73049L);
        //q65Map.put("item", 300000L);
        //q65Map.put("store_sales", 2879987999L);

        /* these are the numbers for a 1GB run */
        q65Map.put("store", 12L);
        q65Map.put("date_dim", 73049L);
        q65Map.put("item", 18000L);
        q65Map.put("store_sales", 2880404L);

        options = new Options();
        options.addOption("h", "help", false, "show help");
        options.addOption("r", "rows", true, "<long> total number of rows (default: " + this.rowCount +")");
        options.addOption("c", "case", true, "case class schema currently supported are: \n" +
                "                             ParquetExample (default), IntWithPayload, and tpcds (WiP). \n" +
                "                             These classes are in ./schema/ in src.");
        options.addOption("f", "caseFile", true, "<String> case class file to compile and load (NYI)");
        options.addOption("o", "output", true, "<String> the output file name (default: " + this.output+")");
        options.addOption("t", "tasks", true, "<int> number of tasks to generate this data (default: " + this.tasks+")");
        options.addOption("p", "partitions", true, "<int> number of output partitions (default: " + this.partitions+")");
        options.addOption("s", "size", true, "<int> any variable payload size, string or payload in IntPayload (default: "
                + this.variableSize+")");
        options.addOption("R", "rangeInt", true, "<int> maximum int value, value for any Int column will be generated " +
                "between [0,rangeInt), (default: " + this.rangeInt+")");
        options.addOption("S", "show", true, "<int> show <int> number of rows (default: " + this.showRows +
                ", zero means do not show)");
        options.addOption("C", "compress", true, "<String> compression type, valid values are: uncompressed, " +
                "snappy, gzip, lzo (default: "
                + this.compressionType+")");

        options.addOption("q", "q65rows", true, "<Long,Long,Long,Long> 4 or less longs, as #rows for store, date_dim, item, store_sales");


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

    public long getRowCount(){
        return this.rowCount;
    }

    public int getPartitions(){
        return this.partitions;
    }

    public int getTasks(){
        return this.tasks;
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

    public int getShowRows(){
        return this.showRows;
    }

    public int getrRangeInt() {
        return this.rangeInt;
    }

    public void show_help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Parqgen", options);
    }

    public HashMap<String, Long>getQ65Map() {
        return q65Map;
    }

    private void showErrorAndExit(String str){
        System.err.println("*********************************************");
        System.err.println("ERROR: " + str);
        System.err.println("*********************************************");
        show_help();
        System.exit(-1);
    }

    public void parse(String[] args) {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        boolean cset = false;
        boolean q65set = false;
        boolean setr = false;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                show_help();
                System.exit(0);
            }

            if (cmd.hasOption("r")) {
                this.rowCount = Long.parseLong(cmd.getOptionValue("r").trim());
                setr = true;
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
                if(this.className.compareToIgnoreCase("tpcds") == 0)
                    q65set = true;
            }

            if (cmd.hasOption("C")) {
                this.compressionType = cmd.getOptionValue("C").trim();
            }

            if (cmd.hasOption("s")) {
                this.variableSize = Integer.parseInt(cmd.getOptionValue("s").trim());
            }

            if (cmd.hasOption("S")) {
                this.showRows = Integer.parseInt(cmd.getOptionValue("S").trim());
            }

            if (cmd.hasOption("p")) {
                this.partitions = Integer.parseInt(cmd.getOptionValue("p").trim());
            }

            if (cmd.hasOption("t")) {
                this.tasks = Integer.parseInt(cmd.getOptionValue("t").trim());
            }

            if (cmd.hasOption("R")) {
                this.rangeInt = Integer.parseInt(cmd.getOptionValue("R").trim());
            }

            if (cmd.hasOption("Q")) {
                /* now we can have 4 or less longs */
                String[] split = cmd.getOptionValue("Q").split(",");
                if(split.length == 4){
                    q65Map.put("store", Long.parseLong(split[0].trim()));
                    q65Map.put("date_dim", Long.parseLong(split[1].trim()));
                    q65Map.put("item", Long.parseLong(split[2].trim()));
                    q65Map.put("store_sales", Long.parseLong(split[3].trim()));

                } else if(split.length == 3){

                    q65Map.put("store", Long.parseLong(split[0].trim()));
                    q65Map.put("date_dim", Long.parseLong(split[1].trim()));
                    q65Map.put("item", Long.parseLong(split[2].trim()));

                } else if(split.length == 2){

                    q65Map.put("store", Long.parseLong(split[0].trim()));
                    q65Map.put("date_dim", Long.parseLong(split[1].trim()));

                } else if(split.length == 1){

                    q65Map.put("store", Long.parseLong(split[0].trim()));

                } else {
                    showErrorAndExit("Failed to parse command line properties for -Q " + cmd.getOptionValue("Q"));
                }
            }

        } catch (ParseException e) {
            showErrorAndExit("Failed to parse command line properties" + e);
        }
        /* do some sanity checks */
        if(this.classFileName != null && (this.className != null && cset) ){
            showErrorAndExit("You cannot define both -f and -c. Please use one");
        }
        /* this will never happen as this.className is defined to a default class */
        if(this.classFileName == null && this.className == null){
            showErrorAndExit("You have to define atleast one class, use either -f XOR -c.");
        }
        /* now say about tpcds */
        if(q65set && setr){
            showErrorAndExit("You are generating TPC-DS data, use -Q instead of -r for #rows.");
        }
    }
}

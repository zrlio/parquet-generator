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

import com.ibm.crail.spark.tools.tpcds.TPCDSOptions;
import org.apache.commons.cli.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by atr on 9/30/16.
 */
public class ParseOptions implements Serializable {
    private Options options;
    private long rowCount;
    private String output;
    private String className;
    private int tasks;
    private int partitions;
    private String compressionType;
    private int variableSize;
    private String banner;
    private int showRows;
    private int rangeInt;
    private boolean affixRandom;
    private String outputFileFormat;
    private Map<String,String> dataSinkOptions;
    private TPCDSOptions tpcdsOptions;

    static <K, V> scala.collection.immutable.Map<K, V> toScalaImmutableMap(java.util.Map<K, V> javaMap) {
        final java.util.List<scala.Tuple2<K, V>> list = new java.util.ArrayList<>(javaMap.size());
        for (final java.util.Map.Entry<K, V> entry : javaMap.entrySet()) {
            list.add(scala.Tuple2.apply(entry.getKey(), entry.getValue()));
        }
        final scala.collection.Seq<Tuple2<K, V>> seq = scala.collection.JavaConverters.asScalaBufferConverter(list).asScala().toSeq();
        return (scala.collection.immutable.Map<K, V>) scala.collection.immutable.Map$.MODULE$.apply(seq);
    }


    public ParseOptions(){
        this.rowCount = 10;
        this.output = "/ParqGenOutput.parquet";
        this.className ="ParquetExample";
        this.tasks = 1;
        this.partitions = 1;
        this.compressionType = "uncompressed";
        this.variableSize = 100;
        this.showRows = 0;
        this.rangeInt = Integer.MAX_VALUE;
        this.affixRandom = false;
        this.outputFileFormat = "parquet";
        this.dataSinkOptions = new Hashtable<String, String>();

        this.tpcdsOptions = new TPCDSOptions("",
                "1",
                "/tpcds",
                "parquet",
                ParseOptions
        .toScalaImmutableMap(this.dataSinkOptions),
                true,
                false,
                false,
                false,
                "",
                4,
                false,
                false
                );
        options = new Options();
        options.addOption("h", "help", false, "show help");
        options.addOption("a", "affix", false, " affix random payload. Means that in each instance of worker, the "+
                " variable payload data will be generated once, and used multiple times (default " + this.affixRandom +")");
        options.addOption("r", "rows", true, "<long> total number of rows (default: " + this.rowCount +")");
        options.addOption("c", "case", true, "case class schema currently supported are: \n" +
                "                             ParquetExample (default), IntWithPayload, and tpcds. \n" +
                "                             These classes are in ./schema/ in src.");
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
        options.addOption("f", "format", true, "<String> output format type (e.g., parquet (default), csv, etc.)");
        options.addOption("O", "options", true, "<str,str> key,value strings that will be passed to the data source of spark in writing." +
        " E.g., for parquet you may want to re-consider parquet.block.size. The default is 128MB (the HDFS block size). ");

//        case class TPCDSOptions(var dsdgen_dir:String = "/home/atr/zrl/external/github/databricks/tpcds-kit/tools/",
//                var scale_factor:String = "1",
//                var data_location:String = "file:/data/tpcds-F1",
//                var format:String = "parquet",
//                var overwrite:Boolean = true,
//                var partitionTables:Boolean = false,
//                var clusterByPartitionColumns:Boolean = false,
//                var filterOutNullPartitionValues:Boolean = false,
//                var tableFilter: String = "",
//                var numPartitions: Int = 4,
//                var useDoubleForDecimal:Boolean = false,
//                var seStringForDate: Boolean = false){
//        }

        options.addOption("tdsd", "dsdgenDir", true, "<String> location of the dsdgen tool");
        options.addOption("tsf", "scaleFactor", true, "<Int> scaling factor");
        // output file name is the same -o
        // format comes from the -f
        options.addOption("tow", "overWrite", true, "<int> true(1, default) or false(0), pass the int");
        options.addOption("tpt", "partitionTable", true, "<int> true(1) or false(0, default), pass the int");
        options.addOption("tcbp", "clusterByPartition", true, "<int> true(1) or false(0, default), pass the int");
        options.addOption("tfon", "filterOutNullPartitionValues", true, "<int> true(1) or false (0, default), pass the int");
        options.addOption("tff", "tableFiler", true, "<String> To choose a specific table to generate from the TPC-DS dataset");
        // num of partition comes from -p
        options.addOption("tdd", "doubleForDecimal", true, "<int> true(1) or false(0, default), pass the int");
        options.addOption("tsd","stringForDate", true, "<int> true(1) or false(0, default), pass the int");

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

    public int getRangeInt() {
        return this.rangeInt;
    }

    private void show_help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("parquet-generator", options);
    }

    public boolean getAffixRandom(){
        return this.affixRandom;
    }

    public Map<String, String> getDataSinkOptions() {
        return this.dataSinkOptions;
    }

    public String getOutputFileFormat(){
        return this.outputFileFormat;
    }

    public TPCDSOptions getTpcdsOptions(){
        return this.tpcdsOptions;
    }

    private void showErrorAndExit(String str){
        show_help();
        System.err.println("************ ERROR *******************");
        System.err.println(str);
        System.err.println("**************************************");
        System.exit(-1);
    }

    public void parse(String[] args) {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                show_help();
                System.exit(0);
            }

            if (cmd.hasOption("a")) {
                this.affixRandom = true;
            }

            if (cmd.hasOption("r")) {
                this.rowCount = Long.parseLong(cmd.getOptionValue("r").trim());
            }

            if (cmd.hasOption("o")) {
                this.output = cmd.getOptionValue("o").trim();
                this.tpcdsOptions.data_location_$eq(this.output);
            }

            if (cmd.hasOption("f")) {
                this.outputFileFormat = cmd.getOptionValue("f").trim();
                this.tpcdsOptions.format_$eq(this.outputFileFormat);
            }

            if (cmd.hasOption("c")) {
                this.className = cmd.getOptionValue("c").trim();
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
                this.tpcdsOptions.numPartitions_$eq(this.partitions);
            }

            if (cmd.hasOption("t")) {
                this.tasks = Integer.parseInt(cmd.getOptionValue("t").trim());
            }

            if (cmd.hasOption("R")) {
                this.rangeInt = Integer.parseInt(cmd.getOptionValue("R").trim());
            }
            if(cmd.hasOption("tdsd")){
                this.tpcdsOptions.dsdgen_dir_$eq(cmd.getOptionValue("tdsd").trim());
             }

            if(cmd.hasOption("tsf")){
                this.tpcdsOptions.scale_factor_$eq(cmd.getOptionValue("tsf").trim());
            }
            if(cmd.hasOption("tow")){
                if(Integer.parseInt(cmd.getOptionValue("tow").trim()) ==  0)
                    this.tpcdsOptions.overwrite_$eq(false);
                else
                    this.tpcdsOptions.overwrite_$eq(true);
            }
            if(cmd.hasOption("tpt")){
                if(Integer.parseInt(cmd.getOptionValue("tpt").trim()) ==  0)
                    this.tpcdsOptions.partitionTables_$eq(false);
                else
                    this.tpcdsOptions.partitionTables_$eq(true);
            }
            if(cmd.hasOption("tcbp")){
                if(Integer.parseInt(cmd.getOptionValue("tcbp").trim()) ==  0)
                    this.tpcdsOptions.clusterByPartitionColumns_$eq(false);
                else
                    this.tpcdsOptions.clusterByPartitionColumns_$eq(true);
            }
            if(cmd.hasOption("tfon")){
                if(Integer.parseInt(cmd.getOptionValue("tfon").trim()) ==  0)
                    this.tpcdsOptions.filterOutNullPartitionValues_$eq(false);
                else
                    this.tpcdsOptions.filterOutNullPartitionValues_$eq(true);
            }
            if(cmd.hasOption("tff")){
                this.tpcdsOptions.tableFilter_$eq(cmd.getOptionValue("tff").trim());
            }
            if(cmd.hasOption("tdd")){
                if(Integer.parseInt(cmd.getOptionValue("tdd").trim()) ==  0)
                    this.tpcdsOptions.useDoubleForDecimal_$eq(false);
                else
                    this.tpcdsOptions.useDoubleForDecimal_$eq(true);
            }
            if(cmd.hasOption("tsd")){
                if(Integer.parseInt(cmd.getOptionValue("tsd").trim()) ==  0)
                    this.tpcdsOptions.seStringForDate_$eq(false);
                else
                    this.tpcdsOptions.seStringForDate_$eq(true);
            }

            if(cmd.hasOption("O")) {
                String[] vals = cmd.getOptionValue("O").split(",");
                if(vals.length !=2) {
                    System.err.println("Failed to parse " + cmd.getOptionValue("P"));
                    System.exit(-1);
                }
                /* otherwise we got stuff */
                dataSinkOptions.put(vals[0].trim(), vals[1].trim());
            }

        } catch (ParseException e) {
            showErrorAndExit("Failed to parse command line properties" + e);
        }

        if(this.className.compareToIgnoreCase("tpcds") == 0){
            if(this.tpcdsOptions.dsdgen_dir().compareToIgnoreCase("") == 0){
                showErrorAndExit("Please set the directory for dsdgen with -tdsd");
            }
        }
    }
}

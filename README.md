# Parquet-Generator 
Parquet file generator for humans

## How to build
```bash
mvn -DskipTests -T 1C install
```

This should give you `parquet-generator-1.0.jar` in your `target` folder.
To build for with-dependencies, you can use: 
```bash
mvn -DskipTests -T 1C clean compile assembly:single
```

## How to run
```bash
./bin/spark-submit --master yarn \ 
--class com.ibm.crail.spark.tools ParquetGenerator \ 
parquet-generator-1.0.jar [OPTIONS]
```

Current options are: 
```bash  
  usage: parquet-generator
   -a,--affix                                   affix random payload. Means that in each instance of worker, 
                                                the variable payload data will be generated once, and used
                                                multiple times (default false)
   -c,--case <arg>                              case class schema currently supported are:
                                                ParquetExample (default),
                                                IntWithPayload, and tpcds.
                                                These classes are in ./schema/ in src.
   -C,--compress <arg>                          <String> compression type, valid values are:
                                                uncompressed, snappy, gzip,
                                                lzo (default: uncompressed)
   -f,--format <arg>                            <String> output format type (e.g., parquet (default), csv, etc.)
   -h,--help                                    show help
   -o,--output <arg>                            <String> the output file name (default: /ParqGenOutput.parquet)
   -O,--options <arg>                           <str,str> key,value strings that will be passed to the data source of spark in
                                                writing. E.g., for parquet you may want to re-consider parquet.block.size. The
                                                default is 128MB (the HDFS block size).
   -p,--partitions <arg>                        <int> number of output partitions (default: 1)
   -r,--rows <arg>                              <long> total number of rows (default: 10)
   -R,--rangeInt <arg>                          <int> maximum int value, value for any Int column will be generated between
                                                [0,rangeInt), (default: 2147483647)
   -s,--size <arg>                              <int> any variable payload size, string or payload in IntPayload (default: 100)
   -S,--show <arg>                              <int> show <int> number of rows (default: 0, zero means do not show)
   -t,--tasks <arg>                             <int> number of tasks to generate this data (default: 1)
   -tcbp,--clusterByPartition <arg>             <int> true(1) or false(0, default), pass the int
   -tdd,--doubleForDecimal <arg>                <int> true(1) or false(0, default), pass the int
   -tdsd,--dsdgenDir <arg>                      <String> location of the dsdgen tool
   -tfon,--filterOutNullPartitionValues <arg>   <int> true(1) or false (0, default), pass the int
   -tow,--overWrite <arg>                       <int> true(1, default) or false(0), pass the int
   -tpt,--partitionTable <arg>                  <int> true(1) or false(0, default), pass the int
   -tsd,--stringForDate <arg>                   <int> true(1) or false(0, default), pass the int
   -tsf,--scaleFactor <arg>                     <Int> scaling factor (default: 1) 
   -ttf,--tableFiler <arg>                      <String> ?
```
An example run would be : 
```bash 
./bin/spark-submit --master yarn \
--class com.ibm.crail.spark.tools.ParquetGenerator parquet-generator-1.0.jar \
-c IntWithPayload -C snappy -o /myfile.parquet -r 84 -s 42 -p 12
```
This will create `984 ( = 12 * 84)` rows for `case class IntWithPayload` as 
`[Int, Array[Byte]]` with 42 bytes byte array, and save this as a parquet file 
format in `/myfile.parquet` in 12 different partitions. 

### How to generate TPC-DS dataset
 This is an example command to generate the dataset with the scaling factor of 2, with 8 tasks but in 2 files 
 (or partitions) when running spark locally. The output goes to crail. 
  
```bash
./bin/spark-submit -v --num-executors 2 --executor-cores 1 --executor-memory 1G --driver-memory 1G --master local 
--class com.ibm.crail.spark.tools.ParquetGenerator 
~/parquet-generator/target/parquet-generator-1.0.jar 
-c tpcds 
-o crail://localhost:9060/tpcds 
-t 8 
-p 2
-tsf 2 
-tdsd ~/tpcds-kit/tools/
```
**Note:** on a cluster the location of dsdgen directory should be accessible on each machine.   

**Acknowledgement:** The data generation logic is derived from https://github.com/databricks/spark-sql-perf

#### How to get and build the `dsdgen-kit` tool 
As described [here](https://github.com/databricks/spark-sql-perf#setup-a-benchmark), the login uses a slightly modified version of the original TPC-DS toolset. It can be downloaded and build from 
 [https://github.com/databricks/tpcds-kit](https://github.com/databricks/tpcds-kit) as
 
```bash
$ git clone https://github.com/databricks/tpcds-kit.git
$ cd ./tpcds-kit/tools/
$ make OS=LINUX
```

## Contributions

PRs are always welcome. Please fork, and make necessary modifications 
you propose, and let us know. 

## Contact 

If you have questions or suggestions, feel free to post at:

https://groups.google.com/forum/#!forum/zrlio-users

or email: zrlio-users@googlegroups.com

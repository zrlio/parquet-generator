# parqgen
Parquet file generator for humans

## How to build
```bash
mvn -DskipTests -T 1C install
```

This should give you `parqgen-1.0.jar` in your `target` folder.
To build for with-dependencies, you can use: 
```bash
mvn -DskipTests -T 1C clean compile assembly:single
```

## How to run
```bash
./bin/spark-submit --master yarn --class com.ibm.crail.spark.tools.Parqgen parqgen-1.0.jar [OPTIONS]
```

Current options are: 
```bash
 usage: Pargen
  -c,--case <arg>          <String> case class schema currently supported are: 
                             ParquetExample (default), IntWithPayload. 
                             These classes are in ./schema/ in src.
  -C,--compress <arg>      <String> compression type, valid values are:
                           uncompressed, snappy, gzip, lzo (default:
                           uncompressed)
  -f,--caseFile <arg>      <String> case class file to compile and load (NYI)  
  -o,--output <arg>        <String> the output file name (default:
                           /test.parquet)  
  -p,--parallelism <arg>   <int> number of partitions (default: 1)
  -r,--rows <arg>          <int> number of rows per partition (default: 10)  
  -s,--size <arg>          <int> any variable payload size, string or
                            payload in IntPayload (default: 100)  
  -h,--help                show help
```
An example run would be : 
```bash 
./bin/spark-submit --master yarn \
--class com.ibm.crail.spark.tools.Parqgen parqgen-1.0.jar \
-c IntWithPayload -C snappy -o /myfile.parquet -r 84 -s 42 -p 12
```
This will create 984 (12 * 84) rows for `case class IntWithPayload` as `[Int, Array[Byte]]` with 42 bytes byte array, and save this as a parquet file format in `/myfile.parquet` in 12 different partitions. 

## Contributions

PRs are always welcome. Please fork, and make necessary modifications 
you propose, and let us know. 

## Contact 

If you have questions or suggestions, feel free to post at:

https://groups.google.com/forum/#!forum/zrlio-users

or email: zrlio-users@googlegroups.com

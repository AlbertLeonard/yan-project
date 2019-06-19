# SparkReadClust (SparRC): a scalable tool for sequence reads clustering using Apache Spark

## Summary:

SpaRC is an Apache Spark-based scalable read clustering application that partitions the sequence reads (transcriptomics or metagenomics) based on their molecule of origin to enable downstream analysis in parallel (e.g., de novo assembly). SpaRC is designed for Pacbio long reads but should work for Illumina pair-end reads, too. 

To cite SpaRC: 

## Requirements:

The development environment uses the following settings (see details in build.sbt)

1. Spark 2.0.1
2. Scala 2.11.8
3. Hadoop 2.7.3
4. JAVA 1.8

To run in your environment, change the versions of dependencies correspondingly and run test to check if it works.

## Build (Scala):

0. install java, sbt, spark and hadoop if necessary, and make sure environment variables of JAVA_HOME, HADOOP_HOME, SPARK_HOME are set.
1. optionally use "sbt -mem 4096 test" to run unit tests (this example uses 4G memory).
2. run "sbt assembly" to make the assembly jar.

## Build (AWS EMR):

0. install git, clone the repo
1. download sbt and extract:

        wget https://github.com/sbt/sbt/releases/download/v0.13.15/sbt-0.13.15.tgz
        tar xzf sbt-0.13.15.tgz
        export PATH=$PATH:./sbt/bin  
    
2. run "sbt package" command in sparRC directory   
    
## Input Format

### Seq

A seq file contains lines of ID, HEAD, SEQ which are separated by TAB ("\t"). An example is

    1	k103_14993672 flag=1 multi=14.0000 len=10505	TCTAAGTAACATTGACACGTAGCACACATAG
    2	k103_13439317 flag=0 multi=59.2840 len=4715	CGCAGTTATCTTTTTCAAATTCTGGCCGATAA

Fasta or fastq files can be converted to .seq file using the provided scripts:

    fastaToSeq.py
    fastqToSeq.py
    
for contigs files that have been sorted, please enable "shuffle" function to balance data partitions
for fasta files with sequence span multiple lines (formatted), using the following command:

    awk '!/^>/ { printf "%s", $0; n = "\n" } /^>/ { print n $0; n = "" } END { printf "%s", n }' multi.fa > singleline.fa

For seq file of old version, the numeric ID may be missing, use the script to add IDs, e.g.

     cat *.seq | scripts/seq_add_id.py > new_seq.txt

### Parquet

_Parquet_ file is a binary compressed format file. It can be converted from _Seq_ file by

    spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.Seq2Parquet target/scala-2.11/LocalCluster-assembly-0.1.jar -i test/small/*.seq -o tmp/parquet_output

Help

    $ spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.Seq2Parquet target/scala-2.11/LocalCluster-assembly-*.jar --help

    Seq2Parquet 0.1
    Usage: Seq2Parquet [options]

      -i, --input <dir>        a local dir where seq files are located in,  or a local file, or an hdfs file
      -p, --pattern <pattern>  if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq
      -o, --output <dir>       output of the top k-mers
      --coalesce               coalesce the output
      -n, --n_partition <value>
                               paritions for the input, only applicable to local files
      --help                   prints this usage text

### Base64

Although Parquet has advantages of data schema and compression etc, use base64 encoding is simpler. Its text format is more flexible and the compression is better.
Convert seq file to base64 format use

    spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.Seq2Base64 target/scala-2.11/LocalCluster-assembly-0.1.jar -i test/small/*.seq -o tmp/base64_output --coalesce

Help

     $ spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.Seq2Base64 target/scala-2.11/LocalCluster-assembly-0.1.jar --help

    Seq2Base64 0.1
    Usage: Seq2Base64 [options]

      -i, --input <dir>        a local dir where seq files are located in,  or a local file, or an hdfs file
      -p, --pattern <pattern>  if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq
      -o, --output <dir>       output of the top k-mers
      --coalesce               coalesce the output
      -n, --n_partition <value>
                               paritions for the input, only applicable to local files
      --help                   prints this usage text

## Count kmers and sort

 __(Optional. If contamination is not required, skip this step)__

Having data on hand (local or hdfs), next step is to counting kmers and sort them by count.
Example for seq files

    spark-submit --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 18G --executor-cores 2 --conf spark.network.timeout=360000 --conf spark.default.parallelism=5000 --conf spark.executor.extraClassPath=target/scala-2.11/LocalCluster-assembly-0.1.jar --conf spark.speculation=true --conf spark.speculation.multiplier=2 --conf spark.eventLog.enabled=false target/scala-2.11/LocalCluster-assembly-0.1.jar KmerCounting --wait 1 -i data/maize14G.seq -o tmp/maize14G_kc_seq_31 --format seq -k 31 -C

Also use "--help" to see the usage

    kmer counting 0.5
    Usage: KmerCounting [options]

      -i, --input <dir>        a local dir where seq files are located in,  or a local file, or an hdfs file
      -p, --pattern <pattern>  if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq
      -o, --output <dir>       output of the top k-mers
      -C, --canonical_kmer     apply canonical kmer
      --format <format>        input format (seq, parquet or base64)
      -n, --n_partition <value>
                               paritions for the input
      --wait <value>           wait $slep second before stop spark session. For debug purpose, default 0.
      -k, --kmer_length <value>
                               length of k-mer
      --n_iteration <value>    #iterations to finish the task. default 1. set a bigger value if resource is low.
      --help                   prints this usage text



## Find the shared reads for kmers(optional removing top kmers first)
Seq example

    spark-submit --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 18G --executor-cores 2 --conf spark.executor.extraClassPath=target/scala-2.11/LocalCluster-assembly-0.1.jar --conf spark.driver.maxResultSize=8g --conf spark.network.timeout=360000 --conf spark.speculation=true --conf spark.default.parallelism=25000 --conf spark.eventLog.enabled=false --conf spark.executor.userClassPathFirst=true --conf spark.driver.userClassPathFirst=true target/scala-2.11/LocalCluster-assembly-0.1.jar KmerMapReads2 --wait 1 --reads data/maize14G.seq --format seq -o tmp/maize14G_kmerreads.txt_31 -k 31 --kmer tmp/maize14G_kc_seq_31 --contamination 0 --min_kmer_count 2 --max_kmer_count 100000 -C --n_iteration 1

Also use "--help" to see the usage

    KmerMapReads 0.5
    Usage: KmerMapReads [options]

      --reads <dir/file>       a local dir where seq files are located in,  or a local file, or an hdfs file
      --kmer <file>            Kmers to be keep. e.g. output from kmer counting
      -p, --pattern <pattern>  if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq
      -o, --output <dir>       output of the top k-mers
      --format <format>        input format (seq, parquet or base64)
      -C, --canonical_kmer     apply canonical kmer
      -c, --contamination <value>
                               the fraction of top k-mers to keep, others are removed likely due to contamination
      --wait <value>           wait $slep second before stop spark session. For debug purpose, default 0.
      -n, --n_partition <value>
                               paritions for the input, only applicable to local files
      -k, --kmer_length <value>
                               length of k-mer
      --min_kmer_count <value>
                               minimum number of reads that shares a kmer
      --max_kmer_count <value>
                               maximum number of reads that shares a kmer. greater than max_kmer_count, however don't be too big
      --n_iteration <value>    #iterations to finish the task. default 1. set a bigger value if resource is low.
      --help                   prints this usage text



## Generate graph edges
Example

    spark-submit --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 18G --executor-cores 2 --conf spark.executor.extraClassPath=target/scala-2.11/LocalCluster-assembly-0.1.jar --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000 --conf spark.speculation=true --conf spark.default.parallelism=5000 --conf spark.eventLog.enabled=true target/scala-2.11/LocalCluster-assembly-0.1.jar GraphGen2 --wait 1 -i tmp/maize14G_kmerreads.txt_31 -o tmp/maize14G_edges.txt_31 --min_shared_kmers 10 --max_shared_kmers 20000 --max_degree 50

Also use "--help" to see the usage

    GraphGen 0.5
    Usage: GraphGen [options]

      -i, --kmer_reads <file>  reads that a kmer shares. e.g. output from KmerMapReads
      -o, --output <dir>       output of the top k-mers
      --wait <value>           wait $slep second before stop spark session. For debug purpose, default 0.
      --max_degree <value>     max_degree of a node
      --min_shared_kmers <value>
                               minimum number of kmers that two reads share
      -n, --n_partition <value>
                               paritions for the input
      --n_iteration <value>    #iterations to finish the task. default 1. set a bigger value if resource is low.
      --help                   prints this usage text




## Find clusters by connected components
Example

    spark-submit --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 18G --executor-cores 2 --conf spark.default.parallelism=5000 --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000 --conf spark.eventLog.enabled=false --conf spark.speculation=true target/scala-2.11/LocalCluster-assembly-0.1.jar GraphCC --wait 1 -i tmp/maize14G_edges.txt -o tmp/maize14G_cc.txt --min_shared_kmers 10 --max_shared_kmers 20000 --min_reads_per_cluster 2 --n_iteration 1 --use_graphframes --top_nodes_ratio 0.05 --big_cluster_threshold 0.05


Also use "--help" to see the usage

    GraphCC 0.5
    Usage: GraphCC [options]

      -i, --edge_file <file>   files of graph edges. e.g. output from GraphGen
      -o, --output <dir>       output file
      -n, --n_partition <value>
                               paritions for the input
      --top_nodes_ratio <value>
                               within a big cluster, top-degree nodes will be removed to re-cluster. This ratio determines how many nodes are removed.
      --big_cluster_threshold <value>
                               Define big cluster. A cluster whose size > #reads*big_cluster_threshold is big
      --wait <value>           wait $slep second before stop spark session. For debug purpose, default 0.
      --n_output_blocks <value>
                               output block number
      --use_graphframes
      --min_shared_kmers <value>
                               minimum number of kmers that two reads share
      --max_shared_kmers <value>
                               max number of kmers that two reads share
      --n_iteration <value>    #iterations to finish the task. default 1. set a bigger value if resource is low.
      --min_reads_per_cluster <value>
                               minimum reads per cluster
      --help                   prints this usage text


## Or find clusters by Label Propagation Algorithm
Example

    spark-submit --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 18G --executor-cores 2 --conf spark.default.parallelism=5000 --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000 --conf spark.eventLog.enabled=false --conf spark.speculation=true --class org.jgi.spark.localcluster.tools.GraphLPA2 target/scala-2.11/LocalCluster-assembly-0.1.jar --wait 1 -i tmp/maize14G_edges.txt_31 -o tmp/maize14G_lpa.txt_31 --min_shared_kmers 10 --max_shared_kmers 20000 --min_reads_per_cluster 2 --max_iteration 50


Also use "--help" to see the usage

    GraphLPA 0.5
    Usage: GraphLPA [options]

      -i, --edge_file <file>   files of graph edges. e.g. output from GraphGen
      -o, --output <dir>       output file
      -n, --n_partition <value>
                               paritions for the input
      --max_iteration <value>  max ietration for LPA
      --wait <value>           wait $slep second before stop spark session. For debug purpose, default 0.
      --n_output_blocks <value>
                               output block number
      --min_shared_kmers <value>
                               minimum number of kmers that two reads share
      --max_shared_kmers <value>
                               max number of kmers that two reads share
      --min_reads_per_cluster <value>
                               minimum reads per cluster
      --help                   prints this usage text


## Generate output
Save the output in text format ("seq-id,cluster-id" per line). Example

    spark-submit --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 20G --executor-cores 2 --conf spark.default.parallelism=54 --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000 --conf spark.speculation=true --conf spark.eventLog.enabled=false target/scala-2.11/LocalCluster-assembly-0.1.jar CCAddSeq --wait 1 -i tmp/maize14G_lpa.txt_31 --reads data/maize14G.seq -o tmp/maize14G_lpaseq.txt_31

Also use "--help" to see the usage

    AddSeq 0.5
    Usage: AddSeq [options]

      -i, --cc_file <file>     files of graph edges. e.g. output from GraphCC
      --reads <dir|file>       a local dir where seq files are located in,  or a local file, or an hdfs file
      -p, --pattern <pattern>  if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq
      --format <format>        input format (seq, parquet or base64)
      -o, --output <dir>       output file
      -n, --n_partition <value>
                               paritions for the input
      --wait <value>           waiting seconds before stop spark session. For debug purpose, default 0.
      --help                   prints this usage text
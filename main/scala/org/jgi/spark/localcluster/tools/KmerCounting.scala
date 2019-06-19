/**
  * Created by Lizhen Shi on 5/13/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster._
import sext._


object KmerCounting extends App with LazyLogging {

  case class Config(input: String = "", output: String = "", n_iteration: Int = 1, pattern: String = "",
                    k: Int = -1, format: String = "seq", sleep: Int = 0, canonical_kmer: Boolean = false,
                    n_partition: Int = 0
                   )

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("KmerCounting") {
      head("kmer counting", Utils.VERSION)

      opt[String]('i', "input").required().valueName("<dir>").action((x, c) =>
        c.copy(input = x)).text("a local dir where seq files are located in,  or a local file, or an hdfs file")

      opt[String]('p', "pattern").valueName("<pattern>").action((x, c) =>
        c.copy(pattern = x)).text("if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq")


      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output of the top k-mers")

      opt[Unit]('C', "canonical_kmer").action((_, c) =>
        c.copy(canonical_kmer = true)).text("apply canonical kmer")


      opt[String]("format").valueName("<format>").action((x, c) =>
        c.copy(format = x)).
        validate(x =>
          if (List("seq", "parquet", "base64").contains(x)) success
          else failure("only valid for seq, parquet or base64")
        ).text("input format (seq, parquet or base64)")


      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")


      opt[Int]('k', "kmer_length").required().action((x, c) =>
        c.copy(k = x)).
        validate(x =>
          if (x >= 11) success
          else failure("k is too small, should not be smaller than 11"))
        .text("length of k-mer")

      opt[Int]("n_iteration").action((x, c) =>
        c.copy(n_iteration = x)).
        validate(x =>
          if (x >= 1) success
          else failure("n should be positive"))
        .text("#iterations to finish the task. default 1. set a bigger value if resource is low.")

      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }

  def logInfo(str: String) = {
    println(str)
    logger.info(str)
  }

  def delete_hdfs_file(filepath: String): Unit = {
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}
    val conf = new Configuration()

    val output = new Path(filepath)
    val hdfs = FileSystem.get(conf)

    // delete existing directory
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
    }
  }


  private def process_iteration(i: Int, readsRDD: RDD[String], config: Config, sc: SparkContext) = {
    val kmer_gen_fun = if (config.canonical_kmer)
      (seq: String) =>  Kmer.generate_kmer(seq = seq, k = config.k)
    else
      (seq: String) =>  Kmer2.generate_kmer(seq = seq, k = config.k)
    val smallKmersRDD =
      process_iteration_spark(i, readsRDD, config, kmer_gen_fun)

    val rdd = smallKmersRDD.filter(_._2 > 1)
    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val raw_count = smallKmersRDD.count
    val kmer_count = rdd.count
    logInfo(s"filter out ${kmer_count} kmers (count>1) from total ${raw_count} kmers")

    (rdd, kmer_count)
  }

  private def process_iteration_spark(i: Int, readsRDD: RDD[String], config: Config, kmer_gen_fun: (String) => Array[DNASeq]): RDD[(DNASeq, Int)] = {
    readsRDD.map(x => kmer_gen_fun(x)).flatMap(x => x)
      .filter(o => Utils.pos_mod(o.hashCode, config.n_iteration) == i)
      .map((_, 1)).reduceByKey(_ + _)
  }


  def run(config: Config, sc: SparkContext): Unit = {

    val start = System.currentTimeMillis
    logInfo(new java.util.Date(start) + ": Program started ...")

    val seqFiles = Utils.get_files(config.input.trim(), config.pattern.trim())
    logger.debug(seqFiles)

    val smallReadsRDD = KmerMapReads2.make_reads_rdd(seqFiles, config.format, config.n_partition, -1, sc).map(_._2)

    smallReadsRDD.cache()

    val values = 0.until(config.n_iteration).map {
      i =>
        val t = process_iteration(i, smallReadsRDD, config, sc)
        t
    }

    if (true) {
      //hdfs
      val rdds = values.map(_._1) //WARNING make sure the rdds in the list are exclusive for kmers
      KmerCounting.delete_hdfs_file(config.output)
      if (false) { //take tops

      } else { //exclude top kmers and 1 kmers
        val rdd = sc.union(rdds)
        val kmer_count = values.map(_._2).sum
        val filteredKmerRDD = rdd.sortBy(x => (-x._2, x._1.hashCode)).map(x => x._1.to_base64 + " " + x._2.toString)
        filteredKmerRDD.saveAsTextFile(config.output)
        logInfo(s"total #kmer=${kmer_count} save results to hdfs ${config.output}")
      }

      //cleanup
      smallReadsRDD.unpersist()
      rdds.foreach(_.unpersist())
      val totalTime1 = System.currentTimeMillis
      logInfo("kmer counting time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
    }
  }


  override def main(args: Array[String]) {

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get


        logInfo(s"called with arguments\n${options.valueTreeString}")
        val conf = new SparkConf().setAppName("Spark Kmer Counting")
        conf.registerKryoClasses(Array(classOf[DNASeq]))
        val sc = new SparkContext(conf)
        run(config, sc)
        if (config.sleep > 0) Thread.sleep(config.sleep * 1000)
        sc.stop()
      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  } //main
}

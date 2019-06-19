/**
  * Created by Lizhen Shi on 5/16/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster._
import sext._

import scala.util.Random


object KmerMapReads2 extends App with LazyLogging {

  case class Config(reads_input: String = "", kmer_input: String = "", output: String = "", pattern: String = "",
                    _contamination: Double = 0.00005, n_iteration: Int = 1, k: Int = -1, min_kmer_count: Int = 2, sleep: Int = 0,
                    max_kmer_count: Int = 200, format: String = "seq", canonical_kmer: Boolean = false, use_native: Boolean = false,
                    n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("KmerMapReads") {
      head("KmerMapReads", Utils.VERSION)

      opt[String]("reads").required().valueName("<dir/file>").action((x, c) =>
        c.copy(reads_input = x)).text("a local dir where seq files are located in,  or a local file, or an hdfs file")

      opt[String]("kmer").valueName("<file>").action((x, c) =>
        c.copy(kmer_input = x)).text("Kmers to be keep. e.g. output from kmer counting ")

      opt[String]('p', "pattern").valueName("<pattern>").action((x, c) =>
        c.copy(pattern = x)).text("if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq")


      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output of the top k-mers")

      opt[String]("format").valueName("<format>").action((x, c) =>
        c.copy(format = x)).
        validate(x =>
          if (List("seq", "parquet", "base64").contains(x)) success
          else failure("only valid for seq, parquet or base64")
        ).text("input format (seq, parquet or base64)")


      opt[Unit]('C', "canonical_kmer").action((_, c) =>
        c.copy(canonical_kmer = true)).text("apply canonical kmer")

      opt[Unit]("use_native").action((_, c) =>
        c.copy(use_native = true)).text("use native c++ lib")

      opt[Double]('c', "contamination").action((x, c) =>
        c.copy(_contamination = x)).
        validate(x =>
          if (x >= 0 && x <= 1) success
          else failure("contamination should be positive and less than 1"))
        .text("the fraction of top k-mers to keep, others are removed likely due to contamination")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")


      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input, only applicable to local files")

      opt[Int]('k', "kmer_length").required().action((x, c) =>
        c.copy(k = x)).
        validate(x =>
          if (x >= 11) success
          else failure("k is too small, should not be smaller than 11"))
        .text("length of k-mer")

      opt[Int]("min_kmer_count").action((x, c) =>
        c.copy(min_kmer_count = x)).
        validate(x =>
          if (x >= 2) success
          else failure("min_kmer_count should be greater than 2"))
        .text("minimum number of reads that shares a kmer")

      opt[Int]("max_kmer_count").action((x, c) =>
        c.copy(max_kmer_count = x)).
        validate(x =>
          if (x >= 2) success
          else failure("max_kmer_count should be greater than 2"))
        .text("maximum number of reads that shares a kmer. greater than max_kmer_count, however don't be too big")

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

  def make_read_id_rdd(file: String, format: String, sc: SparkContext): RDD[(Long, String)] = {
    if (format.equals("parquet")) throw new NotImplementedError
    else {
      var textRDD =
        sc.textFile(file)

      if (format.equals("seq")) {
        textRDD.map {
          line => line.split("\t|\n")
        }.map { x => (x(0).toLong, x(1)) }
      }

      else if (format.equals("base64")) {
        throw new IllegalArgumentException
      }
      else
        throw new IllegalArgumentException
    }

  }

  def make_reads_rdd(file: String, format: String, n_partition: Int, sc: SparkContext): RDD[(Long, String)] = {
    make_reads_rdd(file, format, n_partition, -1, sc)
  }

  def make_reads_rdd(file: String, format: String, n_partition: Int, sample_fraction: Double, sc: SparkContext): RDD[(Long, String)] = {
    if (format.equals("parquet")) throw new NotImplementedError
    else {
      var textRDD = if (n_partition > 0)
        sc.textFile(file, minPartitions = n_partition)
      else
        sc.textFile(file)
      if (sample_fraction > 0) textRDD = textRDD.sample(withReplacement = false, sample_fraction, Random.nextLong())
      if (format.equals("seq")) {

        textRDD.map {
          line => line.split("\t|\n")
        }.map { x => (x(0).toLong, x(2)) }
      }

      else if (format.equals("base64")) {

        textRDD.map {
          line =>
            val a = line.split(",")
            val id = a(0).toInt
            val seq = a.drop(1).map {
              t =>
                val lst = t.split(" ")
                val len = lst(0).toInt
                DNASeq.from_base64(lst(1)).to_bases(len)
            }.mkString("N")
            (id, seq)
        }

      }
      else
        throw new IllegalArgumentException
    }

  }


  private def process_iteration(i: Int, readsRDD: RDD[(Long, String)], topNKmser: RDD[(DNASeq, Boolean)],
                                config: Config, sc: SparkContext) = {

    val kmer_gen_fun = if (config.use_native) {
      (seq: String) => cKmer.generate_kmer(seq = seq, k = config.k, is_canonical = config.canonical_kmer)
    } else {
      if (config.canonical_kmer)
        (seq: String) => Kmer.generate_kmer(seq = seq, k = config.k)
      else
        (seq: String) => Kmer2.generate_kmer(seq = seq, k = config.k)
    }

    val kmer_reads = {
      val kmer_reads_filtered = readsRDD.mapPartitions {
        iterator =>
          iterator.map {
            case (id, seq) =>
              kmer_gen_fun(seq)
                .filter { x =>
                  (Utils.pos_mod(x.hashCode, config.n_iteration) == i)
                }
                .distinct.map(s => (s, Set(id).toSeq))
          }
      }.flatMap(x => x).reduceByKey(_ ++ _).map(u => (u._1, u._2.toSeq)).filter(u => u._2.length >= config.min_kmer_count)

      val kmer_reads_joined =
        if (topNKmser == null) {
          logInfo("no kmer filtered.")
          kmer_reads_filtered
        }
        else {
          kmer_reads_filtered.leftOuterJoin(topNKmser).filter {
            case (_, (_, opt)) =>
              opt match {
                case Some(x) => false
                case None => true
              }
          }.map {
            case (kmer, (reads, _)) => (kmer, reads)
          }
        }

      kmer_reads_joined.map {
        case (kmer, reads) =>
          (kmer, if (reads.length <= config.max_kmer_count) reads else (Random.shuffle(reads).take(config.max_kmer_count)))
      }
    }.persist(StorageLevel.MEMORY_AND_DISK)


    logInfo(s"Generate ${kmer_reads.count} kmers (#count>1 and topN kmer dropped) for iteration $i")


    kmer_reads
  }

  def get_topN_kmers(sc: SparkContext, config: Config): RDD[(DNASeq, Boolean)] = {
    if (config._contamination <= 0) return null

    val _kmers = sc.textFile(config.kmer_input).map(_.split(" ").head.trim()).map(DNASeq.from_base64)
    _kmers.cache()
    val n_kmers = _kmers.count //all distinct kmer count (include len 1 kmern and top kmers)

    val kmers = _kmers.zipWithIndex()

    val topN = (n_kmers * config._contamination).toLong

    val topNKmers = kmers.filter(u => u._2 <= topN).map(u => (u._1, true)).persist(StorageLevel.MEMORY_AND_DISK)

    println("loaded %d kmer counts, take %d top kmers".format(n_kmers, topNKmers.count))

    _kmers.unpersist()
    topNKmers
  }


  def run(config: Config, sc: SparkContext): Unit = {

    val start = System.currentTimeMillis
    logInfo(new java.util.Date(start) + ": Program started ...")

    val seqFiles = Utils.get_files(config.reads_input.trim(), config.pattern.trim())
    logger.debug(seqFiles)

    var topNKmser = get_topN_kmers(sc, config)

    val readsRDD = KmerMapReads2.make_reads_rdd(seqFiles, config.format, config.n_partition, sc)
    readsRDD.cache()


    val rdds = 0.until(config.n_iteration).map {
      i =>
        process_iteration(i, readsRDD, topNKmser, config, sc)
    }

    KmerCounting.delete_hdfs_file(config.output)
    sc.union(rdds).map(x => x)
      //.groupBy(_._1).map(x => (x._1, x._2.flatMap(_._2)))
      .map(x => x._1.to_base64 + " " + x._2.mkString(","))
      .saveAsTextFile(config.output)

    //clean up
    rdds.foreach(_.unpersist(blocking = false))
    readsRDD.unpersist(blocking = false)
    if (topNKmser != null) topNKmser.unpersist(blocking = false)

    val totalTime1 = System.currentTimeMillis
    println("Total process time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
    logInfo("Total process time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
  }


  override def main(args: Array[String]) {

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get
        if (config.reads_input.startsWith("hdfs:") && config.n_partition > 0) {
          logger.error("do not set partition when use hdfs input. Change block size of hdfs instead")
          sys.exit(-1)
        }

        if (config.max_kmer_count < config.min_kmer_count) {
          logger.error("max_kmer_count should not be less than min_kmer_count")
          sys.exit(-1)
        }

        if (config._contamination > 0.0 && config.kmer_input == "") {
          logger.error("kmer input must be specified when  contamination is greater than zero.")
          sys.exit(-1)
        }

        logInfo(s"called with arguments\n${options.valueTreeString}")
        val conf = new SparkConf().setAppName("Spark Kmer Map Reads").set("spark.kryoserializer.buffer.max", "512m")
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

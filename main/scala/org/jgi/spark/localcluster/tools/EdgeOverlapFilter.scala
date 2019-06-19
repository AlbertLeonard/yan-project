/**
  * Created by Lizhen Shi on 5/16/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.jgi.spark.localcluster.{DBManagerSingleton, DNASeq, Utils, cKmer}
import sext._


object EdgeOverlapFilter extends App with LazyLogging {

  case class Config(edge_file: String = "", output: String = "", db: String = "",
                    min_overlap: Int = 31, err_rate: Float = 0f,
                    n_output_blocks: Int = 0, sleep: Int = 0,
                    max_shared_kmers: Int = 20000, min_shared_kmers: Int = 2,
                    n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("EdgeOverlapFilter") {
      head("EdgeOverlapFilter", Utils.VERSION)

      opt[String]('i', "edge_file").required().valueName("<file>").action((x, c) =>
        c.copy(edge_file = x)).text("files of graph edges. e.g. output from GraphGen")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")

      opt[String]("db").required().valueName("<dir>").action((x, c) =>
        c.copy(db = x)).text("rocksdb for the sequences, must be distributed on each node")

      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("partitions for the input")


      opt[Int]("min_overlap").action((x, c) =>
        c.copy(min_overlap = x)).
        validate(x =>
          if (x > 1) success
          else failure("min_overlap should be greater than 2"))
        .text("min_overlap between 2 reads")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $sleep second before stop spark session. For debug purpose, default 0.")


      opt[Int]("n_output_blocks").action((x, c) =>
        c.copy(n_output_blocks = x)).
        validate(x =>
          if (x >= 1) success
          else failure("n_output_blocks should be greater than 0"))
        .text("output block number")

      opt[Double]("err_rate").action((x, c) =>
        c.copy(err_rate = x.toFloat)).
        validate(x =>
          if (x >= 0 && x < 0.5) success
          else failure("err_rate should be greater than 0 and less than 0.5"))
        .text("error rate in seq")

      opt[Int]("min_shared_kmers").action((x, c) =>
        c.copy(min_shared_kmers = x)).
        validate(x =>
          if (x >= 1) success
          else failure("min_shared_kmers should be greater than 2"))
        .text("minimum number of kmers that two reads share")

      opt[Int]("max_shared_kmers").action((x, c) =>
        c.copy(max_shared_kmers = x)).
        validate(x =>
          if (x >= 1) success
          else failure("max_shared_kmers should be greater than 1"))
        .text("max number of kmers that two reads share")

      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }


  def logInfo(str: String) = {
    logger.info(str)
  }

  def fetch_seq(seqid: Int, dbfile: String): String = {
    DBManagerSingleton.instance(dbfile).get(seqid)
  }

  def calc_overlap(seq1_id: Int, seq2_id: Int, min_overlap: Int, err_rate: Float, dbfile: String): Int = {
    val seq1: String = fetch_seq(seq1_id, dbfile)
    val seq2: String = fetch_seq(seq2_id, dbfile)
    //println(seq1,seq2,min_overlap,err_rate)
    cKmer.sequence_overlap(seq1.toString, seq2.toString, min_overlap, err_rate)
  }

  protected def run_overlap(all_edges: RDD[Array[Int]], config: Config, spark: SparkSession,
                            n_reads: Long) = {
    val filtered = all_edges.map {
      x =>
        val score: Int = calc_overlap(x(0), x(1), config.min_overlap, config.err_rate, config.db)

        Array(x(0), x(1), score)
    }.filter(_ (2) > 0).persist(StorageLevel.MEMORY_AND_DISK_SER)

    logInfo(s"After filtering, #records=${filtered.count} are persisted")

    filtered
  }


  def run(config: Config, spark: SparkSession): Long = {

    val sc = spark.sparkContext

    val start = System.currentTimeMillis
    logInfo(new java.util.Date(start) + ": Program started ...")
    val edges =
      (if (config.n_partition > 0)
        sc.textFile(config.edge_file).repartition(config.n_partition)
      else
        sc.textFile(config.edge_file)).
        map { line =>
          line.split(",").map(_.toInt)
        }.filter(x => x(2) >= config.min_shared_kmers && x(2) <= config.max_shared_kmers).map(_.take(2))

    edges.cache()
    logInfo("loaded %d edges".format(edges.count))
    edges.take(5).map(_.mkString(",")).foreach(println)

    val n_reads = edges.flatMap(x => x).distinct().count()
    logInfo(s"total #reads = $n_reads")


    val filtered_edges = run_overlap(edges, config, spark, n_reads)
    KmerCounting.delete_hdfs_file(config.output)

    val result = filtered_edges.map(_.mkString(","))

    (if (config.n_output_blocks > 0) result.repartition(config.n_output_blocks) else result)
      .saveAsTextFile(config.output)
    val result_count = sc.textFile(config.output).count
    logInfo(s"total #records=${result_count} save results to ${config.output}")

    val totalTime1 = System.currentTimeMillis
    logInfo("Processing time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))

    // may be have the bug as https://issues.apache.org/jira/browse/SPARK-15002
    edges.unpersist(blocking = false)
    filtered_edges.unpersist(blocking = false)

    result_count
  }


  override def main(args: Array[String]) {
    val APPNAME = "EdgeOverlapFilter"

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logInfo(s"called with arguments\n${options.valueTreeString}")
        require(config.min_shared_kmers <= config.max_shared_kmers)

        val conf = new SparkConf().setAppName(APPNAME)
        conf.registerKryoClasses(Array(classOf[DNASeq]))

        val spark = SparkSession
          .builder().config(conf)
          .appName(APPNAME)
          .getOrCreate()

        run(config, spark)
        if (config.sleep > 0) Thread.sleep(config.sleep * 1000)
        spark.stop()
      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  } //main
}

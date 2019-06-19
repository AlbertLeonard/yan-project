/**
  * Created by Lizhen Shi on 5/16/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import net.sparc.graph.LPA
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.jgi.spark.localcluster.{DNASeq, Utils}
import sext._


object GraphcosLPA extends App with LazyLogging {

  case class Config(edge_file: String = "", output: String = "", min_shared_kmers: Int = 0, max_iteration: Int = 10,
                    n_output_blocks: Int = 180, weight: String = "none",
                    min_reads_per_cluster: Int = 1, max_shared_kmers: Int = 20000, sleep: Int = 0,
                    n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("GraphcosLPA") {
      head("GraphcosLPA", Utils.VERSION)

      opt[String]('i', "edge_file").required().valueName("<file>").action((x, c) =>
        c.copy(edge_file = x)).text("files of graph edges. e.g. output from GraphGen")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")

      opt[String]("weight").valueName("weight").action((x, c) =>
        c.copy(weight = x)).
        validate(x =>
          if (Seq("none", "edge", "logedge").contains(x.toLowerCase)) success
          else failure("should be one of <none|edge|logedge>"))
        .text("weight schema. <none|edge|logedge>")

      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input")


      opt[Int]("max_iteration").action((x, c) =>
        c.copy(max_iteration = x))
        .text("max ietration for LPA")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")


      opt[Int]("n_output_blocks").action((x, c) =>
        c.copy(n_output_blocks = x)).
        validate(x =>
          if (x >= 0) success
          else failure("n_output_blocks should be greater than 0"))
        .text("output block number")

      opt[Int]("min_shared_kmers").action((x, c) =>
        c.copy(min_shared_kmers = x)).
        validate(x =>
          if (x >= 0) success
          else failure("min_shared_kmers should be >= 0 !"))
        .text("minimum number of kmers that two reads share")

      opt[Int]("max_shared_kmers").action((x, c) =>
        c.copy(max_shared_kmers = x)).
        validate(x =>
          if (x >= 1) success
          else failure("max_shared_kmers should be greater than 1"))
        .text("max number of kmers that two reads share")

      opt[Int]("min_reads_per_cluster").action((x, c) =>
        c.copy(min_reads_per_cluster = x))
        .text("minimum reads per cluster")

      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }


  def lpa_noweight(edgeTuples: RDD[(Int, Int)], config: Config, sqlContext: SQLContext) = {
    val (cc, checkpoint) = LPA.run_wo_weights(edgeTuples, sqlContext, config.max_iteration, checkpoint_dir)
    val clusters = cc.map(x => (x._1.toLong, x._2.toLong))
    (clusters, checkpoint)
  }

  def lpa_weight(edgeTuples: RDD[(Int, Int, Float)], config: Config, sqlContext: SQLContext) = {
    val (cc, checkpoint) = LPA.run_with_weights(edgeTuples, sqlContext, config.max_iteration, checkpoint_dir)
    val clusters = cc.map(x => (x._1.toLong, x._2.toLong))
    (clusters, checkpoint)
  }


  def logInfo(str: String) = {
    logger.info(str)
  }

  protected def run_lpa(all_edges: RDD[Array[Float]], config: Config, spark: SparkSession,
                        n_reads: Long) = {
    val sqlContext = spark.sqlContext

    val (tmp_clusters, checkpoint) = if (config.weight.toLowerCase == "none") {
      val edgeTuples =
        all_edges.flatMap {
          x =>
            List((x(0).toInt, x(1).toInt), (x(1).toInt, x(0).toInt))
        }
      lpa_noweight(edgeTuples, config, sqlContext)
    } else {
      val weight_fun = if (config.weight.toLowerCase == "edge")
        (x: Float) => x
      else
        (x: Float) => math.log(x).toFloat
      val edgeTuples =
        all_edges.flatMap {
          x =>
            List((x(0).toInt, x(1).toInt, weight_fun(x(2))), (x(1).toInt, x(0).toInt, weight_fun(x(2))))
        }
      lpa_weight(edgeTuples, config, sqlContext)
    }

    val clusters = tmp_clusters.map(_.swap)
    clusters.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logInfo(s"#records=${clusters.count} are persisted")

    val final_clusters =
      clusters.groupByKey.filter(_._2.size >= config.min_reads_per_cluster).map(u => (u._1, u._2.toSeq))
    final_clusters.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logInfo(s"Got ${final_clusters.count} clusters")
    clusters.unpersist(blocking = false)
    checkpoint.remove_all()
    final_clusters

  }

  def checkpoint_dir = {
    System.getProperty("java.io.tmpdir")
  }

  def run(config: Config, spark: SparkSession): Long = {

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    sc.setCheckpointDir(checkpoint_dir)

    val start = System.currentTimeMillis
    logInfo(new java.util.Date(start) + ": Program started ...")

    val tmp_edges =
      (if (config.n_partition > 0)
        sc.textFile(config.edge_file).repartition(config.n_partition)
      else
        sc.textFile(config.edge_file)).
        map { line =>
          line.split(",").map(_.toFloat)
        }.filter(x => x(2) >= config.min_shared_kmers && x(2) <= config.max_shared_kmers)
    val edges = if (config.weight.toLowerCase == "none") tmp_edges.map(x => x.take(2)) else tmp_edges
    edges.cache()
    logInfo("loaded %d edges".format(edges.count))
    edges.take(5).map(_.mkString(",")).foreach(println)

    val n_reads = edges.flatMap(x => x).distinct().count()
    logInfo(s"total #reads = $n_reads")
    val final_clusters = run_lpa(edges, config, spark, n_reads)
    KmerCounting.delete_hdfs_file(config.output)

    val result = final_clusters.map(_._2.toList.sorted)
      .map(_.mkString(","))

    result.repartition(config.n_output_blocks).saveAsTextFile(config.output)
    val result_count = sc.textFile(config.output).count
    logInfo(s"total #records=${result_count} save results to ${config.output}")

    val totalTime1 = System.currentTimeMillis
    println("Processing time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
    logInfo("Processing time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))

    // may be have the bug as https://issues.apache.org/jira/browse/SPARK-15002
    edges.unpersist(blocking = false)
    final_clusters.unpersist(blocking = false)

    result_count
  }


  override def main(args: Array[String]) {
    val APPNAME = "GraphcosLPA"

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



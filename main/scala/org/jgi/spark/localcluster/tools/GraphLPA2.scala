/**
  * Created by Lizhen Shi on 5/16/17.
  */
package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy, lib => graphxlib}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.jgi.spark.localcluster.{DNASeq, Utils}
import sext._

import scala.reflect.ClassTag


object GraphLPA2 extends App with LazyLogging {


  case class Config(edge_file: String = "", output: String = "", min_shared_kmers: Int = 2, max_iteration: Int = 10,
                    n_output_blocks: Int = 180,
                    min_reads_per_cluster: Int = 2, max_shared_kmers: Int = 20000, sleep: Int = 0,
                    n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("GraphLPA") {
      head("GraphLPA", Utils.VERSION)

      opt[String]('i', "edge_file").required().valueName("<file>").action((x, c) =>
        c.copy(edge_file = x)).text("files of graph edges. e.g. output from GraphGen")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")

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
          if (x >= 1) success
          else failure("n_output_blocks should be greater than 0"))
        .text("output block number")

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


      opt[Int]("min_reads_per_cluster").action((x, c) =>
        c.copy(min_reads_per_cluster = x))
        .text("minimum reads per cluster")

      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }


  def cc(edgeTuples: RDD[(Int, Int)], config: Config, sqlContext: SQLContext) = {
    cc_graphx(edgeTuples, sqlContext, config.max_iteration)
  }


  def fromEdgeTuples[VD: ClassTag](
                                    rawEdges: RDD[(Int, Int)],
                                    defaultValue: VD,
                                    uniqueEdges: Option[PartitionStrategy] = None,
                                    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, Int] = {
    val edges = rawEdges.map(p => Edge(p._1, p._2, 1))
    val graph = GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
    uniqueEdges match {
      case Some(p) => graph.partitionBy(p).groupEdges((a, b) => a + b)
      case None => graph
    }
  }

  def cc_graphx(edgeTuples: RDD[(Int, Int)], sqlContext: SQLContext, max_iteration: Int) = {
    val graph = fromEdgeTuples(
      edgeTuples, 1.toInt, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER
    )

    val cc = graphxlib.MyLabelPropagation.run(graph, max_iteration)
    val clusters = cc.vertices.map(x => (x._1.toLong, x._2.toLong))
    clusters
  }

  def logInfo(str: String) = {
    logger.info(str)
    println("AAAA " + str)
  }

  protected def run_cc(all_edges: RDD[Array[Int]], config: Config, spark: SparkSession,
                       n_reads: Long) = {
    val sqlContext = spark.sqlContext

    val edgeTuples = all_edges.map {
      x =>
        if (x(0) < x(1)) (x(0), x(1)) else (x(1), x(0))
    }

    logInfo(s"loaded ${edgeTuples.count} edges")

    val clusters = this.cc(edgeTuples, config, sqlContext).map(_.swap)
    clusters.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logInfo(s"#records=${clusters.count} are persisted")

    val final_clusters =
      clusters.groupByKey.filter(_._2.size >= config.min_reads_per_cluster).map(u => (u._1, u._2.toSeq))
    final_clusters.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logInfo(s"Got ${final_clusters.count} clusters")
    clusters.unpersist(blocking = false)
    final_clusters

  }

  def run(config: Config, spark: SparkSession): Long = {

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    sc.setCheckpointDir(System.getProperty("java.io.tmpdir"))

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
    val final_clusters = run_cc(edges, config, spark, n_reads)
    KmerCounting.delete_hdfs_file(config.output)

    val result = final_clusters.map(_._2.toList.sorted)
      .map(_.mkString(","))

    result.repartition(config.n_output_blocks).saveAsTextFile(config.output)
    val result_count = sc.textFile(config.output).count
    logInfo(s"total #records=${result_count} save results to ${config.output}")

    val totalTime1 = System.currentTimeMillis
    logInfo("Processing time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))

    // may be have the bug as https://issues.apache.org/jira/browse/SPARK-15002
    edges.unpersist(blocking = false)
    final_clusters.unpersist(blocking = false)

    result_count
  }


  override def main(args: Array[String]) {
    val APPNAME = "GraphLPA2"

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

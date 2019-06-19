/**
  * Created by Lizhen Shi on 5/16/17.
  */
package org.jgi.spark.localcluster.tools

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes.GraphFrame
import org.jgi.spark.localcluster.{DNASeq, Utils}
import sext._


object GraphCC extends App with LazyLogging {


  case class Config(edge_file: String = "", output: String = "", min_shared_kmers: Int = 2,
                    top_nodes_ratio: Double = 0.1, big_cluster_threshold: Double = 0.2, n_output_blocks: Int = 180,
                    n_iteration: Int = 1, min_reads_per_cluster: Int = 2, max_shared_kmers: Int = 20000, sleep: Int = 0,
                    n_partition: Int = 0, use_graphframes: Boolean = false)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("GraphCC") {
      head("GraphCC", Utils.VERSION)

      opt[String]('i', "edge_file").required().valueName("<file>").action((x, c) =>
        c.copy(edge_file = x)).text("files of graph edges. e.g. output from GraphGen")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")

      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input")

      opt[Double]("top_nodes_ratio").action((x, c) =>
        c.copy(top_nodes_ratio = x))
        .text("within a big cluster, top-degree nodes will be removed to re-cluster. This ratio determines how many nodes are removed. ")

      opt[Double]("big_cluster_threshold").action((x, c) =>
        c.copy(big_cluster_threshold = x))
        .text("Define big cluster. A cluster whose size > #reads*big_cluster_threshold is big")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")

      opt[Int]("n_output_blocks").action((x, c) =>
        c.copy(n_output_blocks = x)).
        validate(x =>
          if (x >= 1) success
          else failure("n_output_blocks should be greater than 0"))
        .text("output block number")


      opt[Unit]("use_graphframes").action((x, c) =>
        c.copy(use_graphframes = true))

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

      opt[Int]("n_iteration").action((x, c) =>
        c.copy(n_iteration = x)).
        validate(x =>
          if (x >= 1) success
          else failure("n should be positive"))
        .text("#iterations to finish the task. default 1. set a bigger value if resource is low.")

      opt[Int]("min_reads_per_cluster").action((x, c) =>
        c.copy(min_reads_per_cluster = x))
        .text("minimum reads per cluster")

      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }


  def cc(edgeTuples: RDD[(Long, Long)], config: Config, sqlContext: SQLContext) = {
    if (config.use_graphframes)
      cc_graphframes(edgeTuples, sqlContext)
    else
      cc_mllib(edgeTuples)

  }

  def cc_mllib(edgeTuples: RDD[(Long, Long)]) = {
    val graph = Graph.fromEdgeTuples(
      edgeTuples, 1
    )
    val cc = graph.connectedComponents()
    val clusters = cc.vertices.map(x => (x._1.toLong, x._2.toLong))
    clusters
  }

  def cc_graphframes(edgeTuples: RDD[(Long, Long)], sqlContext: SQLContext) = {
    val edges = sqlContext.createDataFrame(edgeTuples.map {
      case (src, dst) =>
        (src, dst, 1)
    }).toDF("src", "dst", "cnt")
    val graph = GraphFrame.fromEdges(edges)

    val cc = graph.connectedComponents.run()
    val clusters = cc.select("id", "component").rdd.map(x => (x(0).asInstanceOf[Long], x(1).asInstanceOf[Long]))
    clusters
  }

  private def process_iteration(i: Int, group: (Int, Int), edges: RDD[Array[Long]], config: Config, sqlContext: SQLContext) = {
    val n = config.n_iteration
    val edgeTuples = edges.map {
      x =>
        if (x(0) < x(1)) (x(0), x(1)) else (x(1), x(0))
    }.filter { x =>
      val a = Utils.pos_mod(x._1.toInt, n)
      a >= group._1 && a < group._2
    }

    logInfo(s"group $group loaded ${edgeTuples.count} edges")

    val clusters = this.cc(edgeTuples, config, sqlContext)
    clusters.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logInfo(s"Iteration $i ,#records=${clusters.count} are persisted")

    clusters
  }


  def merge_cc(clusters: RDD[(Long, Long)] /*(v,c)*/
               , raw_edges: RDD[Array[Long]], config: Config, sqlContext: SQLContext): RDD[(Long, Long)] = {
    val edges = raw_edges.map {
      x => (x(0), x(1)) //(v,v)
    }
    val new_edges = edges.join(clusters).map(x => x._2).join(clusters).map(x => x._2) //(c,c)

    val new_clusters = cc(new_edges, config, sqlContext = sqlContext). //(c,x)
      join(clusters.map(_.swap)).map(_._2) //(x,v)

    new_clusters
  }


  protected def run_cc_with_nodes(vertex_groups: List[(Int, Int)], all_edges: RDD[Array[Long]], config: Config, spark: SparkSession,
                                  nodes: Set[Long] = null) = {

    var top_nodes: Set[Long] = null
    val edges =
      if (nodes == null) {
        all_edges
      } else {
        val filtered_edges = all_edges.filter {
          u =>
            u.forall(i => nodes.contains(i))
        }.cache()
        val val_counts = filtered_edges.flatMap(x => x).map(u => (u, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)
        //val_counts.collect.foreach(println)
        val n_nodes = val_counts.count()
        val threshold: Double = math.max(1, n_nodes.toDouble * config.top_nodes_ratio)
        top_nodes = if (false) {
          val_counts.take(threshold.toInt).map(_._1).toSet
        } else {
          val top_degree = val_counts.take(threshold.toInt).reverse.head._2
          val_counts.filter(u => u._2 >= top_degree).map(_._1).collect.toSet
        }

        val this_edges =
          filtered_edges.filter {
            u =>
              u.forall(i => !top_nodes.contains(i))
          }.persist(StorageLevel.MEMORY_AND_DISK)

        logInfo(s"Filtered ${threshold.toInt}[${top_nodes.size}] nodes out of ${n_nodes} noodes with top ratio ${config.top_nodes_ratio} ")
        filtered_edges.unpersist(blocking = false)
        filtered_edges.filter {
          u =>
            u.forall(i => !top_nodes.contains(i))
        }
      }

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    val clusters_list = vertex_groups.indices.map {
      i =>
        process_iteration(i, vertex_groups(i), edges, config, sqlContext)
    }


    val final_clusters = (if (clusters_list.length > 1) {
      val clusters = sc.union(clusters_list)
        .groupByKey.map(x => (x._1, x._2.min))

      merge_cc(clusters, edges, config, sqlContext)
    }
    else {
      clusters_list(0).map(_.swap)
    }).groupByKey.filter(_._2.size >= config.min_reads_per_cluster).map(u => (u._1, u._2.toSeq))
    final_clusters.persist(StorageLevel.MEMORY_AND_DISK)
    logInfo(s"Got ${final_clusters.count} clusters from ${if (nodes == null) null else nodes.size} nodes")
    //clean up
    clusters_list.foreach {
      _.unpersist(blocking = false)
    }
    if (nodes != null) {
      edges.unpersist(blocking = false)
    }

    (final_clusters, top_nodes)
  }

  def logInfo(str: String) = {
    logger.info(str)
    println("AAAA " + str)
  }


  def saveRDD(rdd: RDD[(Long, Seq[Long])], n_partition: Int): String = {
    val tmpdir = System.getProperty("java.io.tmpdir")
    val filename = s"$tmpdir/cc/${UUID.randomUUID().toString}"
    rdd.repartition(n_partition).saveAsObjectFile(filename)
    filename
  }

  def saveTopNodes(topNodes: Set[Long], sc: SparkContext): String = {
    val nodes = topNodes.toSeq
    saveRDD(sc.parallelize(List((nodes.head, nodes))), 1)
  }

  protected def run_cc_with_big_cluster(vertex_groups: List[(Int, Int)], all_edges: RDD[Array[Long]], config: Config,
                                        spark: SparkSession, big_cluster: Set[Long], n_reads: Long): List[String] = {
    val cluster_list = collection.mutable.ListBuffer.empty[String]

    val (clusters, topNodes) = run_cc_with_nodes(vertex_groups, all_edges, config, spark, nodes = big_cluster)
    clusters.persist(StorageLevel.MEMORY_AND_DISK)
    if (topNodes != null) cluster_list.append(saveTopNodes(topNodes, spark.sparkContext))
    val small_clusters = clusters.filter(_._2.length / n_reads.toDouble < config.big_cluster_threshold)
    cluster_list.append(saveRDD(small_clusters, config.n_output_blocks))

    val big_clusters = clusters.filter(_._2.length / n_reads.toDouble >= config.big_cluster_threshold).zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK)
    val n_big = big_clusters.count.toInt
    logInfo(s"Got ${small_clusters.count} small clusters and ${n_big} big clusters")
    clusters.unpersist(blocking = false)

    (0 until n_big).foreach {
      i =>
        val a_cluster = big_clusters.filter(_._2 == i).map(_._1._2).collect()
        require(a_cluster.length == 1)
        val nodes = a_cluster(0).toSet
        val this_clusters = run_cc_with_big_cluster(vertex_groups, all_edges, config, spark, big_cluster = nodes, n_reads = n_reads)
        cluster_list ++= this_clusters
    }
    big_clusters.unpersist(blocking = false)

    cluster_list.toList
  }

  protected def run_cc(vertex_groups: List[(Int, Int)], all_edges: RDD[Array[Long]], config: Config, spark: SparkSession,
                       n_reads: Long): (List[String], RDD[(Long, Seq[Long])]) = {
    val files = run_cc_with_big_cluster(vertex_groups, all_edges, config, spark, big_cluster = null, n_reads = n_reads)
    val sc = spark.sparkContext
    (files, sc.union(files.map(sc.objectFile[(Long, Seq[Long])](_))))
  }

  def run(config: Config, spark: SparkSession): Long = {

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    sc.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    val start = System.currentTimeMillis
    logInfo(new java.util.Date(start) + ": Program started ...")

    val vertex_groups = {
      val n = config.n_iteration
      val a = (0 to n).map(x => math.sqrt(x.toDouble / n))
        .map(x => (x * n).toInt)
      (a.take(a.length - 1), a.tail).zipped.toSet.toList.filter(x => x._1 < x._2).sorted.reverse
    }
    logInfo(s"request ${config.n_iteration} iterations. truly get $vertex_groups groups")

    val edges =
      (if (config.n_partition > 0)
        sc.textFile(config.edge_file).repartition(config.n_partition)
      else
        sc.textFile(config.edge_file)).
        map { line =>
          line.split(",").map(_.toLong)
        }.filter(x => x(2) >= config.min_shared_kmers && x(2) <= config.max_shared_kmers).map(_.take(2))

    edges.cache()
    logInfo("loaded %d edges".format(edges.count))
    edges.take(5).map(_.mkString(",")).foreach(println)

    val n_reads = edges.flatMap(x => x).distinct().count()
    logInfo(s"biggest cluster will have ${(n_reads * config.big_cluster_threshold).toInt} nodes (total $n_reads). In worst case big cluster will iterately be reduced by ${math.ceil(n_reads * config.big_cluster_threshold * config.top_nodes_ratio).toInt}")
    val (files, final_clusters) = run_cc(vertex_groups, edges, config, spark, n_reads)
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
    files.foreach(KmerCounting.delete_hdfs_file(_))

    result_count
  }


  override def main(args: Array[String]) {
    val APPNAME = "GraphCC"

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

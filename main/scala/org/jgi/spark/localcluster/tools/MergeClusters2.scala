package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster._

import scala.collection.mutable.{Map, Set}
import sext._
import breeze.linalg._
import org.apache.spark.sql.SQLContext


object MergeClusters2 extends App with LazyLogging {

  case class Config(cluster_file: String = "", re_cluster_file: String = "", output: String = "",min_reads_per_cluster: Int = 2,
                    percent: Double = 0.0001, n_output_blocks: Int = 180, sleep: Int = 0, n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("MergeClusters2") {
      head("MergeClusters2", Utils.VERSION)

      opt[String]("cluster_file").required().valueName("<file>").action((x, c) =>
        c.copy(cluster_file = x)).text("files of cluster labels. e.g. output from LPA")

      opt[String]("re_cluster_file").required().valueName("<dir>").action((x, c) =>
        c.copy(re_cluster_file = x)).text("a local dir where seq files are located in, or a local file, or an hdfs file")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")

      opt[Int]("min_reads_per_cluster").action((x, c) =>
        c.copy(min_reads_per_cluster = x)).
        validate(x =>
          if (x >= 1) success
          else failure("min_reads_per_cluster should be greater than 2"))
        .text("minimum reads of per_cluster")

      opt[Double]("percent").action((x, c) =>
        c.copy(percent = x))
        .text("percent of per_cluster in all to filter")

      opt[Int]("n_output_blocks").action((x, c) =>
        c.copy(n_output_blocks = x)).
        validate(x =>
          if (x >= 0) success
          else failure("n_output_blocks should be >= 0"))
        .text("output block number")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")

      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input")


      help("help").text("prints this usage text")
    }
    parser.parse(args, Config())
  }

  def logInfo(str: String) = {
    logger.info(str)
    println(str)
  }


  def mergeCluster(raw_clusterRDD:RDD[(Array[Long],Long)], re_clusterRDD:RDD[Array[Long]], sc:SparkContext, config: Config)={
    re_clusterRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logInfo(s"re_clusterRDD is persisted. ***")

    val allReadNum = raw_clusterRDD.flatMap(x => x._1).count()
    // allReadNum.take(5).foreach(println(_))
    logInfo(s"*** ### All reads num is: $allReadNum")

    val raw_combineRDD = raw_clusterRDD.map(x => (x._1,x._1.length,x._2))
      .filter(_._2 <= config.percent*allReadNum).map(x => (x._1, x._3))
    // 上边只要需要结合的cluster，剩下的不需要结合的cluster（单个cluster）组合在一起进行处理。 --可考虑用RDD的减法操作。
    raw_combineRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logInfo(s"raw_combineRDD is persisted. ***")

    val raw_combineBroad = sc.broadcast(raw_combineRDD.collect())
    val single0RDD = raw_clusterRDD.map(x => x.swap).subtractByKey(raw_combineRDD.map(x => x.swap))
    println("*** single0RDD: ***")
    single0RDD.foreach(println(_))

    val reCluIDSet = re_clusterRDD.map{ cluIdSet =>
      cluIdSet.map(_.toInt)
    }.flatMap(x => x)
    // val tmpRDD = single0RDD.map(x => x._2.toInt)
    val cluMergeIDSet = reCluIDSet.subtract(single0RDD.map{x => x._1.toInt})
    val allCluIDSet = sc.parallelize(DenseVector.tabulate(raw_clusterRDD.count().toInt){i => i}.toArray)
    val singleCluIDSet = allCluIDSet.subtract(cluMergeIDSet)

    val raw_cluster = raw_combineBroad.value
    val result0RDD = re_clusterRDD.map{x =>
      var mergeReadsSet: Set[Long] = Set()
      for (cluID <- x){
        for (i <- 0 until(raw_cluster.length)){
          if (cluID == raw_cluster(i)._2) { mergeReadsSet = mergeReadsSet.++(raw_cluster(i)._1.toSet) }
        }
      }
      mergeReadsSet.toArray
    }

    val singleCluIDBroad = sc.broadcast(singleCluIDSet.collect())
    val result1RDD = raw_clusterRDD
      .map{ case(cluID, index) =>
        val singleClu = singleCluIDBroad.value
        var tmpArr: Array[Long] = Array()
        if(singleClu.contains(index.toInt)) { tmpArr = cluID }
        tmpArr
      }

    re_clusterRDD.unpersist(blocking = false)
    raw_combineBroad.unpersist(blocking = false)
    val resRDD = result0RDD.union(result1RDD).filter(_.length >= config.min_reads_per_cluster)
    resRDD
  }


  def checkpoint_dir = {
    System.getProperty("java.io.tmpdir")
  }

  def run(config: Config, sc: SparkContext) {
    sc.setCheckpointDir(checkpoint_dir)

    val start = System.currentTimeMillis
    logInfo(new java.util.Date(start) + ": Program started ...")

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val raw_clusterRDD = (if (config.n_partition > 0)
      sc.textFile(config.cluster_file, minPartitions = config.n_partition)
    else
      sc.textFile(config.cluster_file))
      .map(_.split(",")).map(x => x.map(_.toLong)).zipWithIndex()   // zipWithIndex ...
    raw_clusterRDD.take(5).foreach(println(_))

    //...

    val re_clusterRDD = (if (config.n_partition > 0)
      sc.textFile(config.re_cluster_file, minPartitions = config.n_partition)
    else
      sc.textFile(config.re_cluster_file))
      .map(_.split(",")).map(x => x.map(_.toLong))

    val resultRDD = mergeCluster(raw_clusterRDD, re_clusterRDD, sc, config)
      .map(x => x.toList.sorted).map(_.mkString(","))  //.distinct()     // .distinct() ...


    KmerCounting.delete_hdfs_file(config.output)
    if(config.n_output_blocks > 0){
      resultRDD.repartition(config.n_output_blocks).saveAsTextFile(config.output)
    } else{
      resultRDD.saveAsTextFile(config.output)
    }
    // end

    val totalTime1 = System.currentTimeMillis
    logInfo("Processing time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))

  }


  override def main(args: Array[String]) {
    val APPNAME = "MergeClusters2"

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logInfo(s"called with arguments\n${options.valueTreeString}")

        val conf = new SparkConf().setAppName(APPNAME)
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


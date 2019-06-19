package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster._
// import scala.collection.mutable.{Set,Map}
import sext._
import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions.Rand

object GraphGenCluster extends App with LazyLogging {

  case class Config(cluster_file: String = "", kmer_file: String = "", output: String = "", min_kmer_count: Int = 2,
                    k:Int= 31, n_output_blocks: Int = 180, sleep: Int = 0, n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("GraphGenCluster") {
      head("GraphGenCluster", Utils.VERSION)

      opt[String]("cluster_file").required().valueName("<file>").action((x, c) =>
        c.copy(cluster_file = x)).text("files of cluster labels. e.g. output from LPA")

      opt[String]("kmer_file").required().valueName("<dir>").action((x, c) =>
        c.copy(kmer_file = x)).text("a local dir where seq files are located in,  or a local file, or an hdfs file")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")

      opt[Int]('k', "kmer_length/kmerCount").required().action((x, c) =>
        c.copy(k = x)).
        validate(x =>
          if (x >= 11) success
          else failure("k is too small, should not be smaller than 11"))
        .text("length of k-mer")

      opt[Int]("min_kmer_count").action((x, c) =>
        c.copy(min_kmer_count = x)).
        validate(x =>
          if (x >= 1) success
          else failure("min_shared_kmers should be greater than 2"))
        .text("minimum number of kmers that two reads share")

      opt[Int]("n_output_blocks").action((x, c) =>
        c.copy(n_output_blocks = x)).
        validate(x =>
          if (x >= 0) success
          else failure("n_output_blocks should be greater than 0"))
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

  def unique_kmer(reIdKmRDD: RDD[(Long, String)], clusterRDD: RDD[(Long,Long)], config: Config, sc: SparkContext) = {
    println("**unique_kmer**")
    val joinRDD = clusterRDD.join(reIdKmRDD).map(x=>x._2).groupByKey()
      .persist(StorageLevel.MEMORY_AND_DISK)  //(clusterID, kmerSet)
    //.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logInfo(s"#records: joinRDDs are persisted ***")
    //joinRDD.take(10).foreach(println(_))
    val uniqueKmerRDD = joinRDD.map{
      case(cluId, kmerSet) => {
        val kmerNumMap = scala.collection.mutable.Map[String, Int]()
        for (i <- 0 until kmerSet.size) {
          val km = DNASeq.from_base64(kmerSet.toSeq(i)).toString.substring(0,config.k)   //!!!
          var temp = kmerNumMap.getOrElse(km,0)
          kmerNumMap.put(km,temp+1)
        }   // map1(kmer1->3,kmer2->5)
        val kmerFilter=kmerNumMap.map(x => x._1).toSet
        (cluId, kmerFilter)
      }
    }
    joinRDD.unpersist(blocking = false)   // unpersist
    uniqueKmerRDD
  }

  def gen_cluster(uniqueKmerRDD:RDD[(Long, Set[String])], clusterRDD: RDD[(Long,Long)], config: Config, sc: SparkContext)={
    println("** cluster_gen_edge is running **")

    val clusterBroad = sc.broadcast(clusterRDD.groupBy(_._2).collect())
    val uniqueKmerBroad = sc.broadcast(uniqueKmerRDD.collect())
    val cluster_gen = uniqueKmerRDD.map {
      case (cluID, kmerSet) =>
        val tmp = uniqueKmerBroad.value
        val clb = clusterBroad.value
        var genedge: (Int,Int,Int) = (0,0,0)
        for(i <- 0 until(tmp.length)){
          if(cluID < tmp(i)._1){
            val denominator = min(clb(i)._2.size, clb(cluID.toInt)._2.size)
            val shareKmerCount = (kmerSet.intersect(tmp(i)._2).size*1.0/denominator).toInt
            if(shareKmerCount >= config.min_kmer_count){ genedge = (cluID.toInt,i,shareKmerCount) }
          }
        }
        genedge
    }.filter(_._3 >= config.min_kmer_count)

    cluster_gen
  }



  def checkpoint_dir = {
    System.getProperty("java.io.tmpdir")
  }

  def run(config: Config, sc: SparkContext) {
    sc.setCheckpointDir(checkpoint_dir)

    val start = System.currentTimeMillis
    logInfo(new java.util.Date(start) + ": Program started ...")

    val clusterRDD = (if (config.n_partition > 0)
      sc.textFile(config.cluster_file, minPartitions = config.n_partition)
    else
      sc.textFile(config.cluster_file))
      .map(_.split(",")).map(x => x.map(_.toLong)).zipWithIndex().map {
      case (nodes, idx) =>
        nodes.map((_, idx))
    }.flatMap(x => x)

    clusterRDD.take(5).foreach(println(_))
    //clusterRDD.take(5).map(_.mkString(",")).foreach(println(_))
    val reIdKmRDD =
      (if (config.n_partition > 0)
        sc.textFile(config.kmer_file, minPartitions = config.n_partition)
      else
        sc.textFile(config.kmer_file)
        ).map { line =>
        (line.split(" ")(0), line.split(" ")(1).split(",").map(_.toLong))
      }  //(iIiEiIiIiIg=,[I@7d37ee0c)
        .map{
        case(kmer,readId) => readId.map((_,kmer))
      }.flatMap(x => x)
    reIdKmRDD.take(5).foreach(println(_))
    //for(km <- kmReIdRDD){println(km)}

    val uniqueKmerRDD = unique_kmer(reIdKmRDD,clusterRDD,config,sc)
    uniqueKmerRDD.take(3).foreach(println(_))
    val genEdgeRDD = gen_cluster(uniqueKmerRDD,clusterRDD,config,sc)
    genEdgeRDD.take(10).foreach(println(_))
    KmerCounting.delete_hdfs_file(config.output)
    val finalResRDD = genEdgeRDD.map{
      case (cluID1,cluID2,edge)=>
        cluID1+","+cluID2+","+edge
    }

    if(config.n_output_blocks > 0){
      finalResRDD.repartition(config.n_output_blocks).saveAsTextFile(config.output)
    } else{
      finalResRDD.saveAsTextFile(config.output)
    }

    val totalTime1 = System.currentTimeMillis
    logInfo("Processing time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
  }


  override def main(args: Array[String]) {
    val APPNAME = "GraphGenCluster"

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logInfo(s"called with arguments\n${options.valueTreeString}")

        val conf = new SparkConf().setAppName(APPNAME).set("spark.kryoserializer.buffer.max","1024")
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


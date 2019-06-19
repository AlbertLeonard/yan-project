package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster._
import scala.collection.mutable.Map
import sext._
import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions.Rand

object TNFCos extends App with LazyLogging {

  case class Config(cluster_file: String = "", kmer_file: String = "", tnf_file: String = "" ,
                    output: String = "", k:Int= 31, min_kmer_count: Int = 2, sample: Double = 1.0, filter: Double = 0.00005,
                    n_output_blocks: Int = 180, sleep: Int = 0, n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("ClusterTNF") {
      head("TNFCos", Utils.VERSION)

      opt[String]("cluster_file").required().valueName("<file>").action((x, c) =>
        c.copy(cluster_file = x)).text("files of cluster labels. e.g. output from LPA")

      opt[String]("kmer_file").required().valueName("<dir>").action((x, c) =>
        c.copy(kmer_file = x)).text("a local dir where seq files are located in,  or a local file, or an hdfs file")

      opt[String]("tnf_file").required().valueName("<dir>").action((x, c) =>
        c.copy(tnf_file = x)).text("a local dir where tnf files are located in")

      //      opt[String]("cluster_file_withID").required().valueName("<file>").action((x, c) =>
      //        c.copy(cluster_file_withID = x)).text("AddId files of cluster labels. e.g. output from LPA")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")

      opt[Int]('k', "kmer_length/kmerCount").required().action((x, c) =>
        c.copy(k = x)).
        validate(x =>
          if (x >= 11) success
          else failure("k is too small, should not be smaller than 11"))
        .text("length of k-mer")

      opt[Double]("sample").action((x, c) =>
        c.copy(sample = x))
        .text("sample of unique kmer")

      opt[Double]("filter").action((x, c) =>
        c.copy(filter = x))
        .text("the fraction of top k-mers to keep, others are removed likely due to contamination")

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
      case(cluId, kmerSet)=> {
        val kmerNumMap = scala.collection.mutable.Map[String, Int]()
        for (i <- 0 until kmerSet.size) {
          // val opt = config.k%4
          val km = DNASeq.from_base64(kmerSet.toSeq(i)).toString.substring(0,config.k)   //!!!
          var temp = kmerNumMap.getOrElse(km,0)
          kmerNumMap.put(km,temp+1)
        }   // map1(kmer1->3,kmer2->5)
        //        val uniqueKmerMap = scala.collection.mutable.Map[String, Int]()
        //        for (k <-  kmerNumMap.keys){
        //          val rc = DNASeq.reverse_complement(k)
        //          if(k <= rc){
        //            val number = kmerNumMap(k)+kmerNumMap.getOrElse(rc,0)
        //            if(number >= config.min_kmer_count){ uniqueKmerMap.put(k,number) }
        //          }
        //        }
        //        //val kmerFilter=kmerNumMap.filter(x=>x._2>1)// 过滤掉kmer size=1的kmer
        //        (cluId, uniqueKmerMap)
        val kmerFilter=kmerNumMap.filter(x => x._2>1).map(x => (x._1,1))// 过滤掉kmer size=1的kmer
        (cluId, kmerFilter)
      }
    }
    joinRDD.unpersist(blocking = false)   // unpersist
    uniqueKmerRDD
  }

  def tnf(uniqueKmerRDD:RDD[(Long,Map[String,Int])], config: Config, sc: SparkContext)={
    println("** cluster_tnf is running **")
    //val njoinkmerRDD = uniqueKmerRDD.map(x => (x._1, x._2.keys.mkString("N")))
    val tnf_dictRDD= sc.textFile(config.tnf_file).map {
      line => line.split("\t|\n")
    }.map { x => (x(0),x(1).toInt) }  //(tetra, index) 136种组合
    val tnf_dictBroad = sc.broadcast(tnf_dictRDD.collect())

    //    val tnfRDD = uniqueKmerRDD.sample(false, config.sample).map {
    //      case (id, kmerAndsize) =>
    //        val tnf_dict = tnf_dictBroad.value
    //        val vec = kmerAndsize.map {
    //          subSeq =>
    //            val tnfVec = new Array[Int](136)
    //            val tnfNumMap = scala.collection.mutable.Map[Int, Int]()
    //            (0 until (config.k - 4 + 1)).map {
    //              i =>
    //                val tetra = subSeq._1.substring(i, i + 4)
    //                val index = tnf_dict.filter(x => x._1 == tetra).map(x => x._2)
    //                if (index.length != 0) {
    //                  var temp = tnfNumMap.getOrElse(index.toSeq(0), 0)
    //                  tnfNumMap.put(index.toSeq(0), temp + 1)   //(Int, Int)--(tnf_index, count)
    //                }
    //            }
    //            //tnfNumMap.foreach(x => tnfVec(x._1) = x._2 * subSeq._2)  //每个kmer的TNF
    //            tnfNumMap.foreach(x => tnfVec(x._1) = x._2 )
    //            //println(tnfVec.toList,subSeq._2)
    //            (tnfVec, subSeq._2)
    //        }.reduce{ (x1,x2)=>
    //          val tnfVec = new Array[Int](136)
    //          var kmersum=0
    //          for(i<-0 until x1._1.length){
    //            tnfVec(i)=x1._1(i)+x2._1(i)
    //          }
    //          kmersum=x1._2+x2._2
    //          (tnfVec, kmersum)
    //        }
    //        (id,vec)
    //    }.map { x =>
    //      val tnfVec = new Array[Double](136)
    //        for (i <- 0 until 136) {
    //          tnfVec(i) = (x._2._1(i).toDouble) / (x._2._2 * (config.k + 1).toDouble -4.0)
    //        } //归一化
    //        tnfVec
    //    }

    val tnfRDD = uniqueKmerRDD.sample(false, config.sample)
      .map{x => (x._1,x._2.keys.mkString("N")) }
      .map{case (cluId, cluSeq) =>
        val tnf_dict = tnf_dictBroad.value
        val tnfVec = new Array[Int](136)
        val tnfNumMap = scala.collection.mutable.Map[Int, Int]()
        (0 until (cluSeq.length-3)).map {
          i =>
            val tetra = cluSeq.substring(i, i + 4)
            val index = tnf_dict.filter(x => x._1 == tetra).map(x => x._2)
            if (index.length != 0) {
              var temp = tnfNumMap.getOrElse(index.toSeq(0), 0)
              tnfNumMap.put(index.toSeq(0), temp + 1)   //(Int, Int)--(tnf_index, count)
            }
            else{
              //println("** N is here. **")
            }
            tnfNumMap.foreach(x => tnfVec(x._1) = x._2 )
        }
        val tnfVec2 = new Array[Double](136)
        for (i <- 0 until 136) {
          tnfVec2(i) = (tnfVec(i).toDouble) / (cluSeq.length-3.0)
        }
        tnfVec2
      }
    tnfRDD
  }

  def cosine(tnfRDD: RDD[Array[Double]], config: Config, sc: SparkContext)={
    println("** cosine_function is running. **")
    val Abun = tnfRDD.map{x=>
      val result=for(elem<-x) yield elem.toDouble
      result
    }.zipWithIndex().map(x=>(x._1,x._2.toInt)).persist(StorageLevel.MEMORY_AND_DISK_SER) //.repartition(config.n_partition)
    //matrix_result.unpersist()
    logInfo(s"Abun is persisted. ***")

    val AbunBroad = sc.broadcast(Abun.collect())
    import breeze.linalg._
    import breeze.numerics._
    import scala.collection.mutable.{ArrayBuffer, ListBuffer}
    val cosineSimilarity=Abun.flatMap{
      case(array,id)=>
        val a=DenseVector(array)
        val table=AbunBroad.value
        val buf = new ListBuffer[((Int, Int), Double)]()
        for(i <- 0 until table.length){
          if(id <= table(i)._2) {
            val b = DenseVector(table(i)._1)
            val similar = (a dot b) / (norm(a)*norm(b))
            if(similar >= config.filter)
            {
              buf += (((id, table(i)._2), similar))
            }
          }
        }
        buf
    }.map(x=>(x._1._1,x._1._2,x._2))  //.filter(x=>x._1!=x._2)
    //cosineSimilarity.take(10).foreach(println(_))
    //KmerCounting.delete_hdfs_file(config.output)
    Abun.unpersist(blocking = false)
    cosineSimilarity.map{
      case (clu1,clu2,value)=>
        clu1.toInt+","+clu2.toInt+","+value.toDouble
    }
    cosineSimilarity
    //return cosine_result
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
    //clusterRDD.map(u=>u._2.toInt.toString+"\t"+u._1).saveAsTextFile(config.cluster_file_withID)
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
    val tnfRDD = tnf(uniqueKmerRDD,config,sc)
    //b.map(x=>x.toSeq).foreach(println(_))
    val cossimRDD = cosine(tnfRDD, config, sc)
    cossimRDD.take(10).foreach(println(_))
    KmerCounting.delete_hdfs_file(config.output)
    val finalResRDD = cossimRDD.map{
      case (kmer1,kmer2,value)=>
        kmer1.toInt+","+kmer2.toInt+","+value.toDouble
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
    val APPNAME = "TNFCos"

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

